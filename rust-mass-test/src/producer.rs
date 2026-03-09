use crate::config::Config;
use crate::metrics::ClientMetrics;
use crate::topic::TopicGenerator;
use crate::ui::LogBuffer;
use bytes::Bytes;
use chrono::Utc;
use rumqttc::{AsyncClient, Event, QoS};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::{time, sync::watch};
use uuid::Uuid;

pub async fn run_producer(
    producer_id: usize,
    config: Arc<Config>,
    metrics: Arc<ClientMetrics>,
    mut shutdown_rx: watch::Receiver<bool>,
    mut pause_rx: watch::Receiver<bool>,
    log_buffer: LogBuffer,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_id = format!("pub-{}", Uuid::new_v4().to_string());

    // Outer loop for reconnection attempts
    loop {
        // Check for shutdown before attempting to connect
        if *shutdown_rx.borrow() {
            log_buffer.log(format!("Producer {}: Received shutdown signal. Exiting...", producer_id + 1));
            return Ok(());
        }

        // Create MQTT connection options
        let mut mqttoptions = config.create_mqtt_options(client_id.clone());
        mqttoptions.set_keep_alive(Duration::from_secs(120));
        mqttoptions.set_max_packet_size(100 * 1024, 100 * 1024);

        // For zero-delay mode, use large inflight buffer to maximize throughput
        // For normal mode, use a moderate buffer for efficiency
        let inflight = if config.sleep_ms == 0 { 100 } else { 10 };
        mqttoptions.set_inflight(inflight);

        // Create client and connection
        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        log_buffer.log(format!("Producer {}: Waiting for connection to broker...", producer_id + 1));

        // Wait for CONNACK before proceeding
        let mut connected = false;
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    log_buffer.log(format!("Producer {}: Received shutdown signal before connecting. Disconnecting...", producer_id + 1));
                    let _ = client.disconnect().await;
                    return Ok(());
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(rumqttc::Packet::ConnAck(_ack))) => {
                            log_buffer.log(format!("Producer {}: ✅ Connected to broker", producer_id + 1));
                            metrics.set_connected(true);
                            let is_connected = metrics.is_connected();
                            log_buffer.log(format!("Producer {}: [DEBUG] Connected flag set: {}", producer_id + 1, is_connected));
                            connected = true;
                            break;
                        }
                        Ok(Event::Incoming(_)) => {},
                        Ok(Event::Outgoing(_)) => {},
                        Err(e) => {
                            log_buffer.log(format!("Producer {}: ❌ Connection error: {:?}", producer_id + 1, e));
                            break;
                        }
                    }
                }
            }
        }

        // If we failed to connect, retry after a delay
        if !connected {
            log_buffer.log(format!("Producer {}: Failed to connect, retrying in 2 seconds...", producer_id + 1));
            time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        // Generate topics for this producer (only once per connection)
        let topic_gen = TopicGenerator::new(
            config.topic_prefix.clone(),
            producer_id + 1,
            config.topics_per_node,
            config.max_depth,
        );

        let topics: Vec<String> = topic_gen.generate_all();
        let qos = match config.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        };

        let mode_str = if config.sleep_ms == 0 { "FAST (no delay)" } else { &format!("{} ms", config.sleep_ms) };
        log_buffer.log(format!(
            "Producer {}: Starting with {} topics, mode={}",
            producer_id + 1,
            topics.len(),
            mode_str
        ));

        // Track which topic to publish to
        let mut topic_index = 0;

        // Main publishing loop (inner loop, reconnects on error)
        let mut should_shutdown = false;

        if config.sleep_ms == 0 {
            // Fast mode: publish as fast as possible
            // Spawn eventloop polling on a background task
            let eventloop_task = tokio::spawn(async move {
                loop {
                    if let Ok(event) = eventloop.poll().await {
                        match event {
                            Event::Incoming(rumqttc::Packet::Disconnect) => {
                                break;
                            }
                            _ => {}
                        }
                    } else {
                        break;
                    }
                }
            });

            // Publish as fast as possible in the main loop
            loop {
                // Check if background task crashed
                if eventloop_task.is_finished() {
                    log_buffer.log(format!("Producer {}: Event loop task finished, reconnecting...", producer_id + 1));
                    metrics.set_connected(false);
                    break;
                }

                if *shutdown_rx.borrow() {
                    log_buffer.log(format!("Producer {}: Received shutdown signal. Disconnecting...", producer_id + 1));
                    let _ = client.disconnect().await;
                    metrics.set_connected(false);
                    should_shutdown = true;
                    break;
                }

                if *pause_rx.borrow() {
                    tokio::task::yield_now().await;
                    continue;
                }

                // Generate payload
                let counter = metrics.get_counter();
                let random_value: f64 = fastrand::f64();
                let timestamp = Utc::now().to_rfc3339();

                let payload = json!({
                    "ts": timestamp,
                    "counter": counter,
                    "value": random_value,
                });

                let payload_bytes = Bytes::from(payload.to_string());
                let topic = &topics[topic_index % topics.len()];

                // Publish - the await will naturally allow event loop to process
                match client.publish(topic.clone(), qos, config.retained, payload_bytes).await {
                    Ok(_) => {
                        metrics.increment_published();
                        topic_index += 1;
                    }
                    Err(e) => {
                        log_buffer.log(format!("Producer {}: Publish error: {}", producer_id + 1, e));
                        metrics.set_connected(false);
                        break;
                    }
                }
            }

            eventloop_task.abort();
        } else {
            // Timed mode: publish at configured interval
            let mut publish_timer = time::interval(Duration::from_millis(config.sleep_ms));

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        log_buffer.log(format!("Producer {}: Received shutdown signal. Disconnecting...", producer_id + 1));
                        let _ = client.disconnect().await;
                        metrics.set_connected(false);
                        should_shutdown = true;
                        break;
                    }
                    _ = pause_rx.changed() => {
                        // Pause state changed, just acknowledge it by continuing the loop
                    }
                    event = eventloop.poll() => {
                        match event {
                            Ok(Event::Incoming(rumqttc::Packet::Disconnect)) => {
                                log_buffer.log(format!("Producer {}: ⚠️  Broker sent DISCONNECT, reconnecting...", producer_id + 1));
                                metrics.set_connected(false);
                                break;
                            }
                            Ok(Event::Incoming(_)) => {},
                            Ok(Event::Outgoing(_)) => {},
                            Err(e) => {
                                log_buffer.log(format!("Producer {}: ⚠️  Connection error: {:?}, reconnecting...", producer_id + 1, e));
                                metrics.set_connected(false);
                                break;
                            }
                        }
                    }
                    _ = publish_timer.tick() => {
                        // Check if paused before publishing
                        if *pause_rx.borrow() {
                            continue; // Skip publishing but keep the timer ticking
                        }

                        // Generate payload
                        let counter = metrics.get_counter();
                        let random_value: f64 = fastrand::f64();
                        let timestamp = Utc::now().to_rfc3339();

                        let payload = json!({
                            "ts": timestamp,
                            "counter": counter,
                            "value": random_value,
                        });

                        let payload_bytes = Bytes::from(payload.to_string());

                        let topic = &topics[topic_index % topics.len()];
                        match client.publish(topic.clone(), qos, config.retained, payload_bytes).await {
                            Ok(_) => {
                                metrics.increment_published();
                                // Messages sent immediately due to configured inflight size
                            }
                            Err(e) => {
                                log_buffer.log(format!("Producer {}: Publish error: {}", producer_id + 1, e));
                            }
                        }

                        // Move to next topic for next publish
                        topic_index += 1;
                    }
                }
            }
        }

        // If shutdown was received, exit the outer loop
        if should_shutdown {
            break;
        }

        log_buffer.log(format!("Producer {}: Reconnecting in 2 seconds...", producer_id + 1));
        time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}
