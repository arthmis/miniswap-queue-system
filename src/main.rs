use tokio_postgres::{Config, NoTls};

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    let mut config = Config::new();
    let result = config
        .user("miniswap")
        .dbname("miniswap")
        .password("miniswap")
        .hostaddr(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)))
        .port(7777)
        .connect_timeout(core::time::Duration::from_secs(10))
        .connect(NoTls)
        .await;

    let client = match result {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            client
        }
        Err(e) => {
            return Err(e);
        }
    };

    Ok(())
}
