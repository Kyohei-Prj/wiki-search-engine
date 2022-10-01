// use actix_web::{get, App, HttpResponse, HttpServer, Responder};

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     HttpServer::new(|| App::new().service(search))
//         .bind(("127.0.0.1", 8080))?
//         .run()
//         .await
// }

// #[get("/search")]
// async fn search(keyword: String) -> impl Responder {
//     HttpResponse::Ok().body(keyword)
// }

use elasticsearch::{
    auth::Credentials,
    cert::CertificateValidation,
    http::transport::{SingleNodeConnectionPool, Transport, TransportBuilder},
    http::Url,
    params::Refresh,
    Elasticsearch, Error, IndexParts, SearchParts,
};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // set client
    let credentials = Credentials::Basic("elastic".into(), "elastic".into());
    let url = Url::parse("https://localhost:9200")?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool)
        .auth(credentials)
        .cert_validation(CertificateValidation::None)
        .build()?;
    let client = Elasticsearch::new(transport);

    // execute search
    let response = client
        .search(SearchParts::Index(&["wiki_index"]))
        .from(0)
        .size(10)
        ._source(&["title".into()])
        .body(json!({"query": {"match": {"abstract": "東京"}}}))
        .send()
        .await?;
    let response_body = response.json::<Value>().await?;
    println!("{:?}", response_body);
    

    Ok(())
}
