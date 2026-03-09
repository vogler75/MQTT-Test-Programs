use rumqttc::{AsyncClient, Event, QoS};
use tokio::time;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

use crate::config::Config;
use crate::metrics::ClientMetrics;
use crate::topic::TopicGenerator;
use crate::ui::LogBuffer;

pub async fn run(config: Arc<Config>, metrics: Arc<ClientMetrics>, mut shutdown_rx: watch::Receiver<bool>, log_buffer: LogBuffer) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_id = format!("sub-{}", Uuid::new_v4().to_string());

    // Generate topic generator (once, reuse for reconnections)
    let topic_generator = TopicGenerator::new(
        config.topic_prefix.clone(),
        metrics.id + 1,
        config.topics_per_node,
        config.max_depth,
    );

    // Get topics to subscribe to (once, reuse for reconnections)
    let (all_topics, is_wildcard) = if config.use_wildcard {
        (topic_generator.generate_wildcard_subscriptions(), true)
    } else {
        (topic_generator.generate_all(), false)
    };

    let total_topics_count = all_topics.len();

    // Select a percentage of topics to subscribe to
    let num_topics_to_subscribe = (total_topics_count as f64 * (config.subscribe_percentage as f64 / 100.0)).round() as usize;
    let mut topics_to_subscribe: Vec<String> = all_topics
        .into_iter()
        .collect();
    if !is_wildcard {
        fastrand::shuffle(&mut topics_to_subscribe);
    }
    topics_to_subscribe.truncate(num_topics_to_subscribe);

    let sub_count = topics_to_subscribe.len();

    // Outer loop for reconnection attempts
    loop {
        // Check for shutdown before attempting to connect
        if *shutdown_rx.borrow() {
            log_buffer.log(format!("Subscriber {}: Received shutdown signal. Exiting...", metrics.id + 1));
            return Ok(());
        }

        log_buffer.log(format!("Subscriber {}: [DEBUG] Connecting to {}:{} with client ID {}", metrics.id + 1, &config.broker_host, config.broker_port, client_id));
        let mut mqttoptions = config.create_mqtt_options(client_id.clone());
        mqttoptions.set_keep_alive(Duration::from_secs(120));
        mqttoptions.set_max_packet_size(100 * 1024, 100 * 1024);
        log_buffer.log(format!("Subscriber {}: [DEBUG] MqttOptions configured", metrics.id + 1));

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
        log_buffer.log(format!("Subscriber {}: [DEBUG] Client created, waiting for events", metrics.id + 1));

        // Debug output to show subscription paths
        if config.use_wildcard {
            log_buffer.log(format!("Subscriber {}: Using WILDCARD subscriptions ({} total):", metrics.id + 1, sub_count));
        } else {
            log_buffer.log(format!("Subscriber {}: Using ALL topic subscriptions ({} total):", metrics.id + 1, sub_count));
        }

        // Print subscription paths (limit to first 5 for readability)
        for (idx, topic) in topics_to_subscribe.iter().enumerate() {
            if idx < 5 {
                log_buffer.log(format!("Subscriber {}: [{}] {}", metrics.id + 1, idx + 1, topic));
            } else if idx == 5 {
                log_buffer.log(format!("Subscriber {}: ... and {} more topics", metrics.id + 1, sub_count - 5));
                break;
            }
        }

        log_buffer.log(format!("Subscriber {}: Starting subscription process...", metrics.id + 1));

        let mut topic_index = 0;
        let mut subscribed_count = 0;
        let mut connection_phase = true;
        let mut should_shutdown = false;

        // Event-driven subscription loop: wait for ConnAck, then subscribe to topics one at a time
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    log_buffer.log(format!("Subscriber {}: Received shutdown signal. Disconnecting...", metrics.id + 1));
                    let _ = client.disconnect().await;
                    metrics.set_connected(false);
                    should_shutdown = true;
                    break;
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(rumqttc::Packet::ConnAck(ack))) => {
                            log_buffer.log(format!("Subscriber {}: ✅ Connected to broker: {:?}", metrics.id + 1, ack));
                            metrics.set_connected(true);
                            let is_connected = metrics.is_connected();
                            log_buffer.log(format!("Subscriber {}: [DEBUG] Connected flag set: {}", metrics.id + 1, is_connected));
                            // After ConnAck, subscribe to the first topic
                            if topic_index < sub_count {
                                let topic = &topics_to_subscribe[topic_index];
                                match client.subscribe(topic, QoS::AtMostOnce).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        log_buffer.log(format!("Subscriber {}: ❌ Failed to subscribe to {}: {:?}", metrics.id + 1, topic, e));
                                    }
                                }
                                topic_index += 1;
                            }
                        }
                        Ok(Event::Incoming(rumqttc::Packet::SubAck(_suback))) => {
                            subscribed_count += 1;
                            log_buffer.log(format!("Subscriber {}: ✅ Received SUBACK {}/{}", metrics.id + 1, subscribed_count, sub_count));

                            if subscribed_count >= sub_count {
                                log_buffer.log(format!("Subscriber {}: ✅✅ All {} subscriptions confirmed!", metrics.id + 1, sub_count));
                                connection_phase = false;
                                break;
                            }

                            // Subscribe to next topic
                            if topic_index < sub_count {
                                let topic = &topics_to_subscribe[topic_index];
                                match client.subscribe(topic, QoS::AtMostOnce).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        log_buffer.log(format!("Subscriber {}: ❌ Failed to subscribe to {}: {:?}", metrics.id + 1, topic, e));
                                    }
                                }
                                topic_index += 1;
                            }
                        }
                        Ok(Event::Incoming(_packet)) => {
                            // Ignore other packets during subscription phase
                        }
                        Ok(Event::Outgoing(_outgoing)) => {
                            // Ignore outgoing events
                        }
                        Err(e) => {
                            log_buffer.log(format!("Subscriber {}: ⚠️  Connection error during subscription: {:?}, reconnecting...", metrics.id + 1, e));
                            metrics.set_connected(false);
                            break;
                        }
                    }
                }
            }
        }

        // If shutdown was received, exit the outer loop
        if should_shutdown {
            break;
        }

        // If we're still in connection phase, something went wrong, reconnect
        if connection_phase {
            log_buffer.log(format!("Subscriber {}: Failed to complete subscriptions, reconnecting in 2 seconds...", metrics.id + 1));
            time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        log_buffer.log(format!("Subscriber {}: Subscribed to {} of {} topics.", metrics.id + 1, topics_to_subscribe.len(), total_topics_count));

        // Event loop to process incoming messages
        log_buffer.log(format!("Subscriber {}: Now receiving messages...", metrics.id + 1));
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    log_buffer.log(format!("Subscriber {}: Received shutdown signal. Disconnecting...", metrics.id + 1));
                    let _ = client.disconnect().await;
                    metrics.set_connected(false);
                    should_shutdown = true;
                    break;
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(rumqttc::Packet::Publish(_p))) => {
                            metrics.increment_received();
                        }
                        Ok(Event::Incoming(rumqttc::Packet::Disconnect)) => {
                            log_buffer.log(format!("Subscriber {}: ⚠️  Broker sent DISCONNECT, reconnecting...", metrics.id + 1));
                            metrics.set_connected(false);
                            break;
                        }
                        Ok(Event::Incoming(_packet)) => {
                            // Ignore other packets
                        }
                        Ok(Event::Outgoing(_outgoing)) => {
                            // Ignore outgoing events
                        }
                        Err(e) => {
                            log_buffer.log(format!("Subscriber {}: ⚠️  Connection error: {:?}, reconnecting...", metrics.id + 1, e));
                            metrics.set_connected(false);
                            break;
                        }
                    }
                }
            }
        }

        // If shutdown was received, exit the outer loop
        if should_shutdown {
            break;
        }

        log_buffer.log(format!("Subscriber {}: Reconnecting in 2 seconds...", metrics.id + 1));
        time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}
