use async_nats::jetstream::{self, consumer::PullConsumer};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::{stream::StreamExt, TryStreamExt};

pub fn publish(c: &mut Criterion) {
    let server = nats_server::run_basic_server();
    let mut throughput_group = c.benchmark_group("async-nats: publish throughput");
    throughput_group.sample_size(10);

    let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        throughput_group.throughput(criterion::Throughput::Bytes(*size as u64 * 1000));
        throughput_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc =
                    rt.block_on(async { async_nats::connect(server.client_url()).await.unwrap() });
                let msg = &bmsg[0..*size];

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move { publish_1000_messages(nc, msg).await }
                });
            },
        );
    }
    throughput_group.finish();

    let mut messages_group = c.benchmark_group("async-nats: publish messages amount");
    messages_group.sample_size(10);

    let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        messages_group.throughput(criterion::Throughput::Elements(1000));
        messages_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let nc = async_nats::connect(server.client_url()).await.unwrap();
                    nc.publish("data".to_string(), "data".into()).await.unwrap();
                    nc.flush().await.unwrap();
                    nc
                });
                let msg = &bmsg[0..*size];

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move { publish_1000_messages(nc, msg).await }
                });
            },
        );
    }
    messages_group.finish();
}

pub fn subscribe(c: &mut Criterion) {
    let server = nats_server::run_basic_server();

    let mut subscribe_amount_group = c.benchmark_group("subscribe amount");
    subscribe_amount_group.sample_size(10);

    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        subscribe_amount_group.throughput(criterion::Throughput::Elements(1000));
        subscribe_amount_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let nc = async_nats::connect(server.client_url()).await.unwrap();

                    tokio::task::spawn({
                        let nc = nc.clone();
                        async move {
                            let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
                            let msg = &bmsg[0..*size].to_vec();

                            loop {
                                nc.publish("bench".to_string(), msg.clone().into())
                                    .await
                                    .unwrap();
                            }
                        }
                    });
                    nc.publish("data".to_string(), "data".into()).await.unwrap();
                    nc.flush().await.unwrap();
                    nc
                });

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move { subscribe_1000_messages(nc).await }
                });
            },
        );
    }
    subscribe_amount_group.finish();
}
pub fn pull_consumer_stream(c: &mut Criterion) {
    let server = nats_server::run_server("tests/configs/jetstream.conf");

    let mut pull_consumer_group = c.benchmark_group("pull_stream");

    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        pull_consumer_group.throughput(criterion::Throughput::Elements(1000));
        pull_consumer_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let nc = async_nats::connect(server.client_url()).await.unwrap();

                    tokio::task::spawn({
                        let nc = nc.clone();
                        async move {
                            let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
                            let msg = &bmsg[0..*size].to_vec();

                            loop {
                                nc.publish("bench".to_string(), msg.clone().into())
                                    .await
                                    .unwrap();
                            }
                        }
                    });
                    nc.flush().await.unwrap();
                    nc
                });

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move {
                        let jetstream = jetstream::new(nc.clone());
                        let stream = jetstream
                            .create_stream(jetstream::stream::Config {
                                name: "pull".to_string(),
                                subjects: vec!["bench".to_string()],
                                ..Default::default()
                            })
                            .await
                            .unwrap();
                        let consumer: PullConsumer = stream
                            .get_or_create_consumer(
                                "pull_consumer",
                                jetstream::consumer::pull::Config {
                                    durable_name: Some("pull_consumer".to_string()),
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap();
                        let mut messages = consumer.stream().await.unwrap().take(1000);
                        while let Some(result) = messages.next().await {
                            result.unwrap().ack().await.unwrap();
                        }
                    }
                });
            },
        );
    }
    pull_consumer_group.finish();
}

pub fn pull_consumer_sequence(c: &mut Criterion) {
    let server = nats_server::run_server("tests/configs/jetstream.conf");

    let mut pull_consumer_group = c.benchmark_group("pull_sequence");

    for size in [32, 128, 256, 1024, 4096, 8192].iter() {
        pull_consumer_group.throughput(criterion::Throughput::Elements(1000));
        pull_consumer_group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let nc = rt.block_on(async {
                    let nc = async_nats::connect(server.client_url()).await.unwrap();

                    tokio::task::spawn({
                        let nc = nc.clone();
                        async move {
                            let bmsg: Vec<u8> = (0..32768).map(|_| 22).collect();
                            let msg = &bmsg[0..*size].to_vec();

                            loop {
                                nc.publish("bench".to_string(), msg.clone().into())
                                    .await
                                    .unwrap();
                            }
                        }
                    });
                    nc.publish("data".to_string(), "data".into()).await.unwrap();
                    nc.flush().await.unwrap();
                    nc
                });

                b.to_async(rt).iter(move || {
                    let nc = nc.clone();
                    async move {
                        let jetstream = jetstream::new(nc.clone());
                        let stream = jetstream
                            .create_stream(jetstream::stream::Config {
                                name: "pull".to_string(),
                                subjects: vec!["bench".to_string()],
                                ..Default::default()
                            })
                            .await
                            .unwrap();
                        let consumer: PullConsumer = stream
                            .get_or_create_consumer(
                                "pull_consumer",
                                jetstream::consumer::pull::Config {
                                    durable_name: Some("pull_consumer".to_string()),
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap();
                        let mut iter = consumer.sequence(100).unwrap().take(1000);
                        while let Ok(Some(mut batch)) = iter.try_next().await {
                            while let Ok(Some(_)) = batch.try_next().await {}
                        }
                    }
                });
            },
        );
    }
    pull_consumer_group.finish();
}

async fn publish_1000_messages(nc: async_nats::Client, msg: &'_ [u8]) {
    let msg = msg.to_vec();
    for _i in 0..1000 {
        nc.publish("bench".into(), msg.clone().into())
            .await
            .unwrap();
    }
    nc.flush().await.unwrap();
}

async fn subscribe_1000_messages(nc: async_nats::Client) {
    let mut sub = nc.subscribe("bench".into()).await.unwrap();
    for _ in 0..1000 {
        sub.next().await.unwrap();
    }
}

criterion_group!(
    benches,
    pull_consumer_stream,
    pull_consumer_sequence,
    publish,
    subscribe
);
criterion_main!(benches);
