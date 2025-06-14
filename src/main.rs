use std::{collections::{HashMap, HashSet}, env, error::Error, sync::{Arc, Mutex}, time::Duration};
use libp2p::{multiaddr::Protocol, noise, ping, rendezvous, tcp, yamux, Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, time};
use tokio::{io, io::AsyncBufReadExt};
use axum::{routing::{get, post}, Json, Router};

use futures::prelude::*;
use libp2p::swarm::{SwarmEvent, NetworkBehaviour};
use libp2p::gossipsub;
use libp2p::mdns;

use tracing_subscriber::EnvFilter;

use dotenv;

#[derive(Deserialize)]
struct BroadcastMessage {
    message: String,
}

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    rendezvous: rendezvous::client::Behaviour,
    ping: ping::Behaviour,
}

#[derive(Serialize, Debug, Clone)]
struct PeerStat {
    peer: String,
    address: String,
    ping: Option<u64>,
    last_seen: i64,
}

const HELLO_MSG: &[u8] = b"Hello, NET!";

const NAMESPACE: &str = "botun-aura-network";

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    dotenv::dotenv()?;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let rendezvous_point_address =
        env::var("BOTUN_AURA_RENDEZVOUS_SERVER")?.parse::<Multiaddr>()?;

    let rendezvous_point =
        env::var("BOTUN_AURA_RENDEZVOUS_PEERID")?.parse::<PeerId>()?;

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default
        )?
        .with_behaviour(|key| {
            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                                                                    // signing)
                .build()
                .map_err(io::Error::other)?; // Temporary hack because `build` does not return a proper `std::error::Error`.
            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;

            let rendezvous = rendezvous::client::Behaviour::new(key.clone());
            let ping = ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(10)));

            Ok(MyBehaviour { gossipsub, rendezvous, mdns, ping })

        })?
        .build()
        ;

    // In production the external address should be the publicly facing IP address of the rendezvous
    // point. This address is recorded in the registration entry by the rendezvous point.
    //let external_address = "/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap();
    //swarm.add_external_address(external_address);

    let listener_id = swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    tracing::info!("Listener id: {}", listener_id);
    swarm.dial(rendezvous_point_address.clone())?;

    let peers_set = Arc::new(Mutex::new(HashMap::<PeerId, PeerStat>::new()));
    let (tx, mut rx) = mpsc::channel::<String>(32);

    let peers_clone = peers_set.clone();
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/peers", get({
                let peers = peers_clone.clone();
                move || {
                    let peers = peers.lock().unwrap().clone().values().cloned().collect::<Vec<_>>();
                    async move { Json(peers) }
                }
            }))
            .route("/broadcast", post({
                let tx = tx_clone.clone();
                move |Json(msg): Json<BroadcastMessage>| {
                    let tx = tx.clone();
                    async move {
                        tx.send(msg.message).await.unwrap();
                        Json("Broadcasted")
                    }
                }
            }));

        let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();

        axum::serve(listener, app)
            .await
            .unwrap();
    });

    let mut discover_tick = tokio::time::interval(Duration::from_secs(30));
    let mut cookie = None;

    let mut interval = time::interval(Duration::from_secs(5));

    let topic = gossipsub::IdentTopic::new("test-net-botun-aura");
    // subscribes to our topic
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), HELLO_MSG) {
                    tracing::error!("Error sending mesage: {e:?}");
                }
            }

            _ = discover_tick.tick(), if cookie.is_some() => {
                tracing::info!("Re-discovering peers");
                swarm.behaviour_mut().rendezvous.discover(
                    Some(rendezvous::Namespace::from_static(NAMESPACE)),
                    cookie.clone(),
                    None,
                    rendezvous_point)
            }

            Some(message) = rx.recv() => {
                if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), message) {
                    tracing::error!("Error sending mesage: {e:?}");
                }
            }

            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        tracing::info!("Listening on {address:?}");
                        swarm.add_external_address(address);
                    }

                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        cause: Some(error),
                        ..
                    } if peer_id == rendezvous_point => {
                        tracing::error!("Lost connection to rendezvous point {}", error);
                    }

                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if peer_id == rendezvous_point {
                            if let Err(error) = swarm.behaviour_mut().rendezvous.register(
                                rendezvous::Namespace::from_static(NAMESPACE),
                                rendezvous_point,
                                None,
                            ) {
                                tracing::error!("Failed to register: {error}");
                                anyhow::bail!("Failed to register: {error}");
                            }
                            tracing::info!("Connection established with rendezvous point {}", peer_id);

                            tracing::info!("Discovering peers in '{NAMESPACE}' namespace");

                            swarm.behaviour_mut().rendezvous.discover(
                                Some(rendezvous::Namespace::from_static(NAMESPACE)),
                                None,
                                None,
                                rendezvous_point,
                            );
                        } else {
                            tracing::info!("New connection from {}", peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Ping(ping::Event {
                        peer,
                        result: Ok(rtt),
                        ..
                    })) if peer != rendezvous_point => {
                        tracing::info!(%peer, "Ping is {}ms", rtt.as_millis());
                        let mut peers = peers_set.lock().unwrap();
                        if let Some(peer_stat) = peers.get_mut(&peer) {
                            peer_stat.ping = Some(rtt.as_millis() as u64);
                            peer_stat.last_seen = chrono::Local::now().timestamp();
                        }
                    }

                    SwarmEvent::Behaviour(event) => {
                        tracing::trace!("{event:?}");
                        match event {

                            MyBehaviourEvent::Rendezvous(rendezvous::client::Event::Discovered {
                                registrations,
                                cookie: new_cookie,
                                ..
                            }) => {
                                cookie.replace(new_cookie);

                                for registration in registrations {
                                    for address in registration.record.addresses() {
                                        let peer = registration.record.peer_id();
                                        tracing::info!(%peer, %address, "Discovered peer");

                                        let p2p_suffix = Protocol::P2p(peer);
                                        let address_with_p2p =
                                            if !address.ends_with(&Multiaddr::empty().with(p2p_suffix.clone())) {
                                                address.clone().with(p2p_suffix)
                                            } else {
                                                address.clone()
                                            };

                                        swarm.dial(address_with_p2p).unwrap();
                                    }
                                }
                            }

                            MyBehaviourEvent::Mdns(mdns::Event::Discovered(peers)) => {
                                for (peer_id, addr) in peers {
                                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                                    peers_set.lock().unwrap().insert(peer_id,
                                        PeerStat {
                                            peer: peer_id.to_string(),
                                            address: addr.to_string(),
                                            ping: None,
                                            last_seen: chrono::Local::now().timestamp()
                                        });

                                }
                            },
                            MyBehaviourEvent::Mdns(mdns::Event::Expired(peers)) => {
                                for (peer_id, _addr) in peers {
                                    swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                                    peers_set.lock().unwrap().remove(&peer_id);
                                }
                            },
                            MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                                propagation_source: peer_id,
                                message_id: _,
                                message }) => {
                                tracing::info!("Message from {peer_id}: {}",
                                    String::from_utf8_lossy(&message.data));
                            },
                            _ => {
                                tracing::trace!("ignored");
                            }
                        }
                    },
                    _ => {}
                }
            }
        }
    }

}

