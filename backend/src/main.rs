use actix_web::{get, App, HttpResponse, HttpServer, Responder};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(search))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}

#[get("/search")]
async fn search(keyword: String) -> impl Responder {
    HttpResponse::Ok().body(keyword)
}
