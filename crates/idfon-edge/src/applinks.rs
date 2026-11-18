//! iOS universal links on the apex origin (`https://idfon.net`).
//!
//! - `GET /.well-known/apple-app-site-association` serves the AASA file that
//!   lets iOS open `https://idfon.net/go/*` in the idfon app.
//! - `GET /go/<rest>` is the fallback when the app does not open the link (not
//!   installed, or the link was opened in a browser): a small page with an
//!   "Open in idfon" button pointing at `idfon://<rest>`.
//!
//! `https://idfon.net/go/<rest>` is the same link as `idfon://<rest>` (grammar:
//! `ios/Idfon/IdfonURL.swift`). Only the apex matches; `<ref>.idfon.net`
//! virtual hosts are never served here. No auth: both responses are static
//! apart from echoing the request path back into the page.

use std::future::Future;
use std::pin::Pin;

use http::{HeaderMap, Method, Response, StatusCode, Uri};
use hyper::body::Incoming;
use idfon_gateway::{LocalRoute, ResponseBody};

/// `<team id>.<bundle id>` of the iOS app.
pub const DEFAULT_APP_ID: &str = "RV27HPQNMF.com.idfon.ios";

const AASA_PATH: &str = "/.well-known/apple-app-site-association";
const LINK_PREFIX: &str = "/go/";

pub struct AppLinksRoutes {
    aasa: String,
}

impl AppLinksRoutes {
    pub fn new(app_id: &str) -> Self {
        Self { aasa: aasa_json(app_id) }
    }
}

/// The AASA document: only `/go/*` opens the app; every other path stays on
/// the web (edge resources, `/_push/*`, `/_files/*`).
pub fn aasa_json(app_id: &str) -> String {
    serde_json::json!({
        "applinks": {
            "details": [{
                "appIDs": [app_id],
                "components": [{ "/": "/go/*", "comment": "idfon:// links over https" }]
            }]
        }
    })
    .to_string()
}

/// `/go/<rest>` -> `idfon://<rest>`; `None` when `<rest>` is empty.
pub fn idfon_url(path: &str) -> Option<String> {
    let rest = path.strip_prefix(LINK_PREFIX)?;
    (!rest.is_empty()).then(|| format!("idfon://{rest}"))
}

/// The page shown when a `/go/*` link opens in a browser.
pub fn fallback_html(target: Option<&str>) -> String {
    let button = match target {
        Some(url) => format!(
            r#"<p><a class="btn" href="{}">Open in idfon</a></p>"#,
            escape(url)
        ),
        None => String::new(),
    };
    format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>idfon</title>
<style>
body{{font:17px -apple-system,system-ui,sans-serif;margin:0;padding:48px 24px;text-align:center;color:#111;background:#fff}}
@media (prefers-color-scheme:dark){{body{{color:#eee;background:#000}}}}
.btn{{display:inline-block;padding:12px 24px;border-radius:12px;background:#0a7d4f;color:#fff;text-decoration:none;font-weight:600}}
p{{max-width:28em;margin:16px auto}}
</style></head><body>
<h1>idfon</h1>
{button}
<p>This link opens in the idfon app. If nothing happens, install idfon on this iPhone and tap the link again.</p>
</body></html>
"#
    )
}

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

fn respond(content_type: &str, body: String) -> Response<ResponseBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .header(http::header::CACHE_CONTROL, "public, max-age=3600")
        .body(idfon_gateway::bytes_body(body))
        .expect("static headers")
}

impl LocalRoute for AppLinksRoutes {
    fn matches(&self, method: &Method, account: &str, path: &str) -> bool {
        (method == Method::GET || method == Method::HEAD)
            && account.is_empty()
            && (path == AASA_PATH || path.starts_with(LINK_PREFIX))
    }

    fn handle(
        &self,
        _method: &Method,
        _account: &str,
        path: &str,
        _headers: &HeaderMap,
        _uri: &Uri,
        _body: Incoming,
    ) -> Pin<Box<dyn Future<Output = Response<ResponseBody>> + Send + '_>> {
        let response = if path == AASA_PATH {
            respond("application/json", self.aasa.clone())
        } else {
            respond(
                "text/html; charset=utf-8",
                fallback_html(idfon_url(path).as_deref()),
            )
        };
        Box::pin(async move { response })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aasa_names_the_app_and_only_go_paths() {
        let doc: serde_json::Value = serde_json::from_str(&aasa_json(DEFAULT_APP_ID)).unwrap();
        let details = &doc["applinks"]["details"][0];
        assert_eq!(details["appIDs"][0], DEFAULT_APP_ID);
        assert_eq!(details["components"][0]["/"], "/go/*");
        assert_eq!(details["components"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn matches_apex_only() {
        let routes = AppLinksRoutes::new(DEFAULT_APP_ID);
        assert!(routes.matches(&Method::GET, "", AASA_PATH));
        assert!(routes.matches(&Method::HEAD, "", "/go/dial/alice"));
        assert!(!routes.matches(&Method::GET, "somepeer", AASA_PATH));
        assert!(!routes.matches(&Method::GET, "somepeer", "/go/dial/alice"));
        assert!(!routes.matches(&Method::POST, "", "/go/dial/alice"));
        assert!(!routes.matches(&Method::GET, "", "/gone"));
        assert!(!routes.matches(&Method::GET, "", "/_push/health"));
    }

    #[test]
    fn go_path_maps_to_idfon_scheme() {
        assert_eq!(idfon_url("/go/dial/alice").as_deref(), Some("idfon://dial/alice"));
        assert_eq!(idfon_url("/go/peer/a/b").as_deref(), Some("idfon://peer/a/b"));
        assert_eq!(idfon_url("/go/"), None);
        assert_eq!(idfon_url("/other"), None);
    }

    #[test]
    fn fallback_escapes_the_link() {
        let html = fallback_html(Some(r#"idfon://x"><script>"#));
        assert!(html.contains(r#"href="idfon://x&quot;&gt;&lt;script&gt;""#));
        assert!(!html.contains("<script>"));
        assert!(!fallback_html(None).contains("Open in idfon"));
    }
}
