import XCTest
@testable import IdfonStore

/// The SQLite message log: rows written as messages change, kept in log
/// order across updates and reopening, searched, and windowed.
final class MessageDatabaseTests: XCTestCase {
    private var url: URL!

    override func setUp() {
        super.setUp()
        url = FileManager.default.temporaryDirectory
            .appendingPathComponent("messages-\(UUID().uuidString)/messages.sqlite")
    }

    override func tearDown() {
        try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        super.tearDown()
    }

    private func row(_ id: String, chat: String = "bob", at seconds: TimeInterval = 0, _ body: String = "{}") -> MessageDatabase.Row {
        MessageDatabase.Row(id: id, chat: chat, timestamp: Date(timeIntervalSince1970: 1_800_000_000 + seconds), body: Data(body.utf8))
    }

    func testRowsKeepLogOrderAcrossUpdatesAndReopening() throws {
        var db = try XCTUnwrap(MessageDatabase(url: url))
        XCTAssertTrue(db.apply(rows: [row("a"), row("b"), row("c")], deletes: []))
        // Updating "a" keeps its place; a new row goes last.
        XCTAssertTrue(db.apply(rows: [row("a", "{\"edited\":true}"), row("d")], deletes: ["b"]))
        db = try XCTUnwrap(MessageDatabase(url: url))
        XCTAssertEqual(db.all().map(\.id), ["a", "c", "d"])
        XCTAssertEqual(String(decoding: db.all()[0].body, as: UTF8.self), "{\"edited\":true}")
        XCTAssertEqual(db.count, 3)
    }

    func testOneChatsRowsAreItsOwnOldestFirst() throws {
        let db = try XCTUnwrap(MessageDatabase(url: url))
        db.apply(rows: [row("1", chat: "bob", at: 30), row("2", chat: "room", at: 10), row("3", chat: "bob", at: 20)], deletes: [])
        XCTAssertEqual(db.rows(inChat: "bob").map(\.id), ["3", "1"])
        XCTAssertEqual(db.rows(inChat: "room").map(\.id), ["2"])
        XCTAssertTrue(db.rows(inChat: "nobody").isEmpty)
    }

    func testSearchFindsSubstringsIgnoringCaseAndAccents() throws {
        let db = try XCTUnwrap(MessageDatabase(url: url))
        func text(_ id: String, _ chat: String, _ text: String?, at seconds: TimeInterval) -> MessageDatabase.Row {
            var r = row(id, chat: chat, at: seconds)
            r.text = text
            return r
        }
        db.apply(rows: [
            text("1", "bob", "Dinner at Ristorante Luigi's café?", at: 1),
            text("2", "room", "점심 먹었어요", at: 2),
            text("3", "bob", "100% sure, see file_name.txt", at: 3),
            text("4", "bob", nil, at: 4),            // a system line: never found
            text("5", "zoe", "Luigi said \"yes\"", at: 5),
        ], deletes: [])
        XCTAssertEqual(db.search("uigi").map(\.id), ["5", "1"], "substring, newest first")
        XCTAssertEqual(db.search("CAFE").map(\.id), ["1"], "case and accents")
        XCTAssertEqual(db.search("먹었").map(\.id), ["2"], "two characters scan the text")
        XCTAssertEqual(db.search("uigi", inChat: "bob").map(\.id), ["1"], "one chat")
        XCTAssertEqual(db.search("0%").map(\.id), ["3"], "LIKE wildcards are literal")
        XCTAssertEqual(db.search("e_n").map(\.id), ["3"], "an underscore is literal")
        XCTAssertEqual(db.search("\"yes\"").map(\.id), ["5"], "quotes are literal")
        XCTAssertTrue(db.search("nothing like this").isEmpty)
        // The index follows updates and deletes.
        db.apply(rows: [text("1", "bob", "Lunch at noon", at: 1)], deletes: ["5"])
        XCTAssertTrue(db.search("uigi").isEmpty)
        XCTAssertEqual(db.search("noon").map(\.id), ["1"])
    }

    func testWindowKeepsRecentRowsPerChatAndEveryPendingOne() throws {
        let db = try XCTUnwrap(MessageDatabase(url: url))
        var rows = (0..<10).map { row("a\($0)", chat: "a", at: Double($0)) }
        rows[1].pending = true                      // an old message still waiting
        rows += (0..<3).map { row("b\($0)", chat: "b", at: Double(100 + $0)) }
        rows[11].merged = ["e1", "e2"]
        db.apply(rows: rows, deletes: [])
        XCTAssertEqual(db.window(perChat: 4).map(\.id), ["a1", "a6", "a7", "a8", "a9", "b0", "b1", "b2"])
        XCTAssertEqual(db.chats(over: 4), ["a"])
        XCTAssertEqual(db.history(ofChat: "a").map(\.id), (0..<10).map { "a\($0)" })
        let keys = db.keys()
        XCTAssertEqual(keys.count, 13)
        XCTAssertEqual(keys.first { $0.id == "b1" }?.merged, ["e1", "e2"])
    }

    /// A migration-sized write: 50,000 rows in one transaction.
    func testFiftyThousandRowsWriteQuickly() throws {
        let db = try XCTUnwrap(MessageDatabase(url: url))
        let rows = (0..<50_000).map { row("m\($0)", chat: "c\($0 % 500)", at: Double($0), "{\"text\":\"message \($0)\"}") }
        let started = Date()
        XCTAssertTrue(db.apply(rows: rows, deletes: []))
        let elapsed = Date().timeIntervalSince(started)
        XCTAssertLessThan(elapsed, 3, "writing 50k rows took \(elapsed) s")
        XCTAssertEqual(db.count, 50_000)
        XCTAssertEqual(db.rows(inChat: "c7").count, 100)
    }
}
