import Foundation
import os
import SQLite3

/// The message log in SQLite: one row per message, written as messages
/// change instead of rewriting the whole history.
///
/// It lives in Application Support (`Idfon/messages/<identity>/`, beside the
/// daemon's `Idfon/daemon`): the
/// device keeps all the history it can, and iOS never clears it to free
/// space, as it may clear Caches. Keeping history beyond the device is a
/// paid service's job, not this file's.
///
/// A row is the message's stored JSON (`body`) plus the columns the store
/// reads without decoding: its chat (room id or peer id), time, and `seq`,
/// the log's order. The log only appends, updates in place and removes, so a
/// row keeps the `seq` it got when it was first written.
///
/// Not thread-safe: an app's store drives one instance from one serial queue.
public final class MessageDatabase {
    public struct Row: Equatable, Sendable {
        public let id: String
        public let chat: String
        public let timestamp: Date
        public let body: Data
        /// What search finds the message by; nil if search skips it.
        public var text: String?
        /// Event ids folded into this row (streamed transcript parts): the
        /// replay dedupe needs them without decoding the body.
        public var merged: [String]
        /// Still sending or waiting: always loaded, so the outbox sees it.
        public var pending: Bool

        public init(id: String, chat: String, timestamp: Date, body: Data, text: String? = nil,
                    merged: [String] = [], pending: Bool = false) {
            self.id = id
            self.chat = chat
            self.timestamp = timestamp
            self.body = body
            self.text = text
            self.merged = merged
            self.pending = pending
        }
    }

    /// Bumped when the schema changes; an older database is dropped and
    /// rebuilt (no users yet, so no migrations).
    private static let schemaVersion = 3

    private var db: OpaquePointer?
    private var upsert: OpaquePointer?
    private var delete: OpaquePointer?
    public let url: URL
    private static let log = Logger(subsystem: "app.idfon", category: "MessageDatabase")

    /// Where an identity's log lives.
    public static func location(identity: String) -> URL {
        let name = identity.isEmpty ? "default" : identity.replacingOccurrences(of: "/", with: "_")
        return FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Idfon/messages/\(name)", isDirectory: true)
            .appendingPathComponent("messages.sqlite")
    }

    /// Opens (creating if needed) the database at `url`; nil if SQLite can't.
    public init?(url: URL) {
        self.url = url
        do {
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        } catch {
            Self.log.error("idfon messages db: no directory for \(url.path, privacy: .public): \(error.localizedDescription, privacy: .public)")
        }
        guard sqlite3_open_v2(url.path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK else {
            Self.log.error("idfon messages db: open failed at \(url.path, privacy: .public): \(Self.message(self.db), privacy: .public)")
            sqlite3_close(db)
            return nil
        }
        guard exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;") else { close(); return nil }
        if version != Self.schemaVersion {
            exec("""
                DROP TRIGGER IF EXISTS messages_ai; DROP TRIGGER IF EXISTS messages_ad; DROP TRIGGER IF EXISTS messages_au;
                DROP TABLE IF EXISTS messages_fts; DROP TABLE IF EXISTS messages;
                """)
        }
        // `text` is indexed for search by trigram FTS5: any substring, in any
        // script, ignoring case and accents, like the in-memory search was.
        // Triggers keep the index in step with the table.
        let schema = """
            CREATE TABLE IF NOT EXISTS messages (
                id   TEXT PRIMARY KEY,
                chat TEXT NOT NULL,
                ts   REAL NOT NULL,
                seq  INTEGER NOT NULL,
                body BLOB NOT NULL,
                text TEXT,
                merged TEXT,
                pending INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS messages_seq ON messages(seq);
            CREATE INDEX IF NOT EXISTS messages_chat_ts ON messages(chat, ts);
            CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
                INSERT INTO messages_fts(rowid, text) VALUES (new.rowid, new.text);
            END;
            CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
                INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
            END;
            CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
                INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
                INSERT INTO messages_fts(rowid, text) VALUES (new.rowid, new.text);
            END;
            PRAGMA user_version = \(Self.schemaVersion);
            """
        // Accent folding for trigram needs SQLite 3.45; older systems (iOS 18)
        // get plain trigram rather than no database.
        let fts = "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(text, content='messages', content_rowid='rowid', tokenize="
        guard exec(fts + "'trigram remove_diacritics 1');", quiet: true) || exec(fts + "'trigram');"),
              exec(schema) else { close(); return nil }
        // A new row takes the next seq; an existing one keeps its own.
        upsert = prepare("""
            INSERT INTO messages (id, chat, ts, seq, body, text, merged, pending)
            VALUES (?1, ?2, ?3, (SELECT COALESCE(MAX(seq), 0) + 1 FROM messages), ?4, ?5, ?6, ?7)
            ON CONFLICT(id) DO UPDATE SET chat = excluded.chat, ts = excluded.ts, body = excluded.body,
                text = excluded.text, merged = excluded.merged, pending = excluded.pending
            """)
        delete = prepare("DELETE FROM messages WHERE id = ?1")
        guard upsert != nil, delete != nil else { close(); return nil }
    }

    deinit { close() }

    private func close() {
        sqlite3_finalize(upsert)
        sqlite3_finalize(delete)
        upsert = nil
        delete = nil
        sqlite3_close(db)
        db = nil
    }

    /// Writes `rows` (in order: new rows get increasing seq) and removes
    /// `deletes`, in one transaction. False if anything failed (rolled back).
    @discardableResult
    public func apply(rows: [Row], deletes: [String]) -> Bool {
        guard !rows.isEmpty || !deletes.isEmpty else { return true }
        guard exec("BEGIN IMMEDIATE") else { return false }
        var ok = true
        for row in rows where ok {
            sqlite3_reset(upsert)
            sqlite3_bind_text(upsert, 1, row.id, -1, Self.transient)
            sqlite3_bind_text(upsert, 2, row.chat, -1, Self.transient)
            sqlite3_bind_double(upsert, 3, row.timestamp.timeIntervalSince1970)
            row.body.withUnsafeBytes { bytes in
                _ = sqlite3_bind_blob(upsert, 4, bytes.baseAddress, Int32(bytes.count), Self.transient)
            }
            if let text = row.text {
                sqlite3_bind_text(upsert, 5, text, -1, Self.transient)
            } else {
                sqlite3_bind_null(upsert, 5)
            }
            if row.merged.isEmpty {
                sqlite3_bind_null(upsert, 6)
            } else {
                sqlite3_bind_text(upsert, 6, row.merged.joined(separator: "\n"), -1, Self.transient)
            }
            sqlite3_bind_int(upsert, 7, row.pending ? 1 : 0)
            ok = sqlite3_step(upsert) == SQLITE_DONE
        }
        for id in deletes where ok {
            sqlite3_reset(delete)
            sqlite3_bind_text(delete, 1, id, -1, Self.transient)
            ok = sqlite3_step(delete) == SQLITE_DONE
        }
        if ok, exec("COMMIT") { return true }
        Self.log.error("idfon messages db: write failed: \(Self.message(self.db), privacy: .public)")
        exec("ROLLBACK")
        return false
    }

    /// Every row, in log order.
    public func all() -> [Row] {
        rows("SELECT id, chat, ts, body, text FROM messages ORDER BY seq")
    }

    /// The latest `perChat` rows of every chat, plus every pending row, in
    /// log order: what the store keeps in memory.
    public func window(perChat: Int) -> [Row] {
        rows("""
            SELECT id, chat, ts, body, text FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY chat ORDER BY seq DESC) AS rank FROM messages)
            WHERE rank <= \(perChat) OR pending = 1 ORDER BY seq
            """)
    }

    /// Chats with more than `count` rows: the window holds only part of them.
    public func chats(over count: Int) -> Set<String> {
        guard let statement = prepare("SELECT chat FROM messages GROUP BY chat HAVING COUNT(*) > \(count)") else { return [] }
        defer { sqlite3_finalize(statement) }
        var chats = Set<String>()
        while sqlite3_step(statement) == SQLITE_ROW { chats.insert(String(cString: sqlite3_column_text(statement, 0))) }
        return chats
    }

    /// Every row's key and the event ids folded into it, without bodies.
    public func keys() -> [(id: String, merged: [String])] {
        guard let statement = prepare("SELECT id, merged FROM messages") else { return [] }
        defer { sqlite3_finalize(statement) }
        var keys: [(String, [String])] = []
        while sqlite3_step(statement) == SQLITE_ROW {
            let id = String(cString: sqlite3_column_text(statement, 0))
            let merged = sqlite3_column_text(statement, 1).map { String(cString: $0).components(separatedBy: "\n") } ?? []
            keys.append((id, merged))
        }
        return keys
    }

    /// One chat's rows, oldest first.
    public func rows(inChat chat: String) -> [Row] {
        rows("SELECT id, chat, ts, body, text FROM messages WHERE chat = ?1 ORDER BY ts, seq", bind: [chat])
    }

    /// One chat's rows in log order.
    public func history(ofChat chat: String) -> [Row] {
        rows("SELECT id, chat, ts, body, text FROM messages WHERE chat = ?1 ORDER BY seq", bind: [chat])
    }

    /// Rows whose text contains `phrase` (ignoring case and accents), newest
    /// first, optionally in one chat. Three characters or more use the
    /// trigram index; shorter phrases (many Korean words are two) scan the
    /// text column, still without decoding a message.
    public func search(_ phrase: String, inChat chat: String? = nil, limit: Int = 100) -> [Row] {
        let chatClause = chat == nil ? "" : " AND m.chat = ?2"
        let sql: String
        let pattern: String
        if phrase.count >= 3 {
            sql = "SELECT m.id, m.chat, m.ts, m.body, m.text FROM messages_fts JOIN messages m ON m.rowid = messages_fts.rowid WHERE messages_fts MATCH ?1\(chatClause) ORDER BY m.ts DESC LIMIT \(limit)"
            pattern = "\"" + phrase.replacingOccurrences(of: "\"", with: "\"\"") + "\""
        } else {
            sql = "SELECT m.id, m.chat, m.ts, m.body, m.text FROM messages m WHERE m.text LIKE ?1 ESCAPE '\\'\(chatClause) ORDER BY m.ts DESC LIMIT \(limit)"
            let escaped = phrase.replacingOccurrences(of: "\\", with: "\\\\")
                .replacingOccurrences(of: "%", with: "\\%").replacingOccurrences(of: "_", with: "\\_")
            pattern = "%" + escaped + "%"
        }
        return rows(sql, bind: [pattern] + (chat.map { [$0] } ?? []))
    }

    public var count: Int {
        guard let statement = prepare("SELECT COUNT(*) FROM messages") else { return 0 }
        defer { sqlite3_finalize(statement) }
        return sqlite3_step(statement) == SQLITE_ROW ? Int(sqlite3_column_int64(statement, 0)) : 0
    }

    // MARK: - SQLite plumbing

    private static let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    private static func message(_ db: OpaquePointer?) -> String {
        db.flatMap { sqlite3_errmsg($0) }.map { String(cString: $0) } ?? "no database"
    }

    @discardableResult
    private func exec(_ sql: String, quiet: Bool = false) -> Bool {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            if quiet { return false }
            Self.log.error("idfon messages db: \(String(sql.prefix(40)), privacy: .public)… failed: \(Self.message(self.db), privacy: .public)")
            return false
        }
        return true
    }

    private func prepare(_ sql: String) -> OpaquePointer? {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK else {
            Self.log.error("idfon messages db: prepare failed: \(Self.message(self.db), privacy: .public)")
            return nil
        }
        return statement
    }

    private var version: Int {
        guard let statement = prepare("PRAGMA user_version") else { return 0 }
        defer { sqlite3_finalize(statement) }
        return sqlite3_step(statement) == SQLITE_ROW ? Int(sqlite3_column_int64(statement, 0)) : 0
    }

    private func rows(_ sql: String, bind values: [String] = []) -> [Row] {
        guard let statement = prepare(sql) else { return [] }
        defer { sqlite3_finalize(statement) }
        for (index, value) in values.enumerated() {
            sqlite3_bind_text(statement, Int32(index + 1), value, -1, Self.transient)
        }
        var result: [Row] = []
        while sqlite3_step(statement) == SQLITE_ROW {
            let id = String(cString: sqlite3_column_text(statement, 0))
            let chat = String(cString: sqlite3_column_text(statement, 1))
            let ts = sqlite3_column_double(statement, 2)
            let count = Int(sqlite3_column_bytes(statement, 3))
            let body = sqlite3_column_blob(statement, 3).map { Data(bytes: $0, count: count) } ?? Data()
            let text = sqlite3_column_text(statement, 4).map { String(cString: $0) }
            result.append(Row(id: id, chat: chat, timestamp: Date(timeIntervalSince1970: ts), body: body, text: text))
        }
        return result
    }
}
