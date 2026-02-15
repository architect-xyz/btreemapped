#![allow(dead_code)]

use anyhow::Result;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio_postgres::NoTls;

pub async fn setup_postgres_container(
) -> Result<(testcontainers::ContainerAsync<GenericImage>, u16)> {
    let container = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "wal_sender_timeout=5s",
        ])
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(5432).await?;

    Ok((container, host_port))
}

pub async fn create_postgres_client(
    host: &str,
    port: u16,
) -> Result<tokio_postgres::Client> {
    let conn_str = format!(
        "host={} port={} user=postgres password=postgres dbname=testdb",
        host, port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}
