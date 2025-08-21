use crate::*;
use std::fs::File;
use std::io::Write;

const S1: &str = "2o79pTXHaK1FTPZeBiJo2lCgXW_P0ULjX_5Div_2qxU";

const K1: &str = "m-U7gdxW1A647O-4wkuCWOvtGGVfHEsxNScFKiL8-k8";
const K2: &str = "v9I5GT3xVKPcaa4uyd2pcuJromf5zv1-OaahYOLBAWY";

#[derive(Debug)]
#[allow(dead_code)]
struct DecodeAgent {
    space: String,
    agent: String,
    created_at: i64,
    expires_at: i64,
    is_tombstone: bool,
    encoded: String,
    signature: String,
    test_prop: String,
}

impl<'de> serde::Deserialize<'de> for DecodeAgent {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Out {
            agent_info: String,
            signature: String,
        }

        let out: Out = serde::Deserialize::deserialize(deserializer)?;

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Inn {
            space: String,
            agent: String,
            created_at: String,
            expires_at: String,
            is_tombstone: bool,
            test_prop: String,
        }

        let inn: Inn = serde_json::from_str(&out.agent_info).unwrap();

        Ok(Self {
            space: inn.space,
            agent: inn.agent,
            created_at: inn.created_at.parse().unwrap(),
            expires_at: inn.expires_at.parse().unwrap(),
            is_tombstone: inn.is_tombstone,
            encoded: out.agent_info,
            signature: out.signature,
            test_prop: inn.test_prop,
        })
    }
}

#[derive(Debug)]
struct PutInfoRes {
    info: String,
    agent: String,
}

struct PutInfo<'lt> {
    pub addr: std::net::SocketAddr,
    pub space: &'lt str,
    pub space_url: &'lt str,
    pub agent_seed: &'lt str,
    pub agent_url: Option<&'lt str>,
    pub created_at: i64,
    pub expires_at: i64,
    pub is_tombstone: bool,
    pub final_agent_pk: Option<&'lt str>,
    pub signature: Option<&'lt str>,
    pub test_prop: &'lt str,
    pub use_tls: bool,
}

impl Default for PutInfo<'_> {
    fn default() -> Self {
        let created_at = now();
        let expires_at = created_at
            + std::time::Duration::from_secs(60 * 20).as_micros() as i64;
        Self {
            addr: ([0, 0, 0, 0], 0).into(),
            space: S1,
            space_url: S1,
            agent_seed: K1,
            agent_url: None,
            created_at,
            expires_at,
            is_tombstone: false,
            final_agent_pk: None,
            signature: None,
            test_prop: "<none>",
            use_tls: false,
        }
    }
}

impl PutInfo<'_> {
    fn call(self) -> std::io::Result<PutInfoRes> {
        use base64::prelude::*;
        use ed25519_dalek::*;

        let seed: [u8; 32] = BASE64_URL_SAFE_NO_PAD
            .decode(self.agent_seed)
            .unwrap()
            .try_into()
            .unwrap();
        let sign = SigningKey::from_bytes(&seed);
        let pk =
            BASE64_URL_SAFE_NO_PAD.encode(VerifyingKey::from(&sign).as_bytes());

        let agent_info = serde_json::to_string(&serde_json::json!({
            "space": self.space,
            "agent": match self.final_agent_pk {
                Some(fapk) => fapk,
                None => &pk,
            },
            "createdAt": self.created_at.to_string(),
            "expiresAt": self.expires_at.to_string(),
            "isTombstone": self.is_tombstone,
            "testProp": self.test_prop,
        }))
        .unwrap();

        let signature = BASE64_URL_SAFE_NO_PAD
            .encode(sign.sign(agent_info.as_bytes()).to_bytes());

        let info = serde_json::to_string(&serde_json::json!({
            "agentInfo": agent_info,
            "signature": match self.signature {
                Some(signature) => signature,
                None => &signature,
            }
        }))
        .unwrap();

        let addr = format!(
            "http{}://{:?}/bootstrap/{}/{}",
            if self.use_tls { "s" } else { "" },
            self.addr,
            self.space_url,
            match self.final_agent_pk {
                Some(fapk) => fapk,
                None => match self.agent_url {
                    Some(agent_url) => agent_url,
                    None => &pk,
                },
            },
        );

        let agent_builder =
            ureq::Agent::config_builder().http_status_as_error(false);
        let agent = if self.use_tls {
            agent_builder
                .tls_config(
                    ureq::tls::TlsConfig::builder()
                        .disable_verification(true)
                        .build(),
                )
                .build()
        } else {
            agent_builder.build()
        }
        .new_agent();

        let res = agent.put(&addr).send(&info).unwrap();
        if res.status().is_success() {
            let res = res
                .into_body()
                .read_to_string()
                .map_err(std::io::Error::other)?;
            if res != "{}" {
                return Err(std::io::Error::other("InvalidResponse"));
            }
            Ok(PutInfoRes { info, agent: pk })
        } else {
            let res = res
                .into_body()
                .read_to_string()
                .map_err(std::io::Error::other)?;

            Err(std::io::Error::other(res))
        }
    }
}

#[test]
fn happy_bootstrap_put_get() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let PutInfoRes { info, .. } = PutInfo {
        addr: s.listen_addrs()[0],
        ..Default::default()
    }
    .call()
    .unwrap();

    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    println!("{addr}");
    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    println!("{res}");

    // make sure it is valid json and only contains one entry
    let r: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();
    assert_eq!(1, r.len());

    // make sure that info byte-wise matches our put
    assert_eq!(format!("[{info}]"), res);
}

#[test]
fn happy_empty_server_health() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();
    let addr = format!("http://{:?}/health", s.listen_addrs()[0]);
    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    assert_eq!("{}", res);
}

#[test]
fn happy_empty_server_bootstrap_get() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();
    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    assert_eq!("[]", res);
}

#[test]
fn invalid_auth() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();
    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    let res = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
        .get(&addr)
        .header("Authorization", "Bearer bob")
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    assert!(format!("{res:?}").contains("Unauthorized"), "Got: {res:?}");
}

#[test]
fn valid_auth() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();
    let addr = format!("http://{:?}/authenticate", s.listen_addrs()[0]);
    let token = ureq::put(&addr)
        .send(&b"hello"[..])
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();

    let token = String::from_utf8_lossy(&token.as_bytes()[14..57]);

    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    let res = ureq::get(&addr)
        .header("Authorization", &format!("Bearer {token}"))
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();

    assert_eq!("[]", res);
}

#[test]
fn tombstone_will_not_put() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let _ = PutInfo {
        addr: s.listen_addrs()[0],
        is_tombstone: true,
        ..Default::default()
    }
    .call()
    .unwrap();

    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    assert_eq!("[]", res);
}

#[test]
fn tombstone_old_is_ignored() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let _ = PutInfo {
        addr: s.listen_addrs()[0],
        ..Default::default()
    }
    .call()
    .unwrap();

    let _ = PutInfo {
        addr: s.listen_addrs()[0],
        created_at: now()
            - std::time::Duration::from_secs(60).as_micros() as i64,
        is_tombstone: true,
        ..Default::default()
    }
    .call()
    .unwrap();

    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    let res: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();
    assert_eq!(1, res.len());
}

#[test]
fn tombstone_deletes_correct_agent() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    // -- put agent1 -- //

    let PutInfoRes {
        info: info1,
        agent: agent1,
    } = PutInfo {
        addr: s.listen_addrs()[0],
        ..Default::default()
    }
    .call()
    .unwrap();

    // -- put agent2 -- //

    let PutInfoRes {
        info: info2,
        agent: agent2,
    } = PutInfo {
        addr: s.listen_addrs()[0],
        agent_seed: K2,
        ..Default::default()
    }
    .call()
    .unwrap();

    // -- tombstone agent1 -- //

    let PutInfoRes {
        info: info1_t,
        agent: agent1_t,
    } = PutInfo {
        addr: s.listen_addrs()[0],
        is_tombstone: true,
        ..Default::default()
    }
    .call()
    .unwrap();

    // -- validate test -- //

    assert_eq!(agent1, agent1_t);
    assert_ne!(agent1, agent2);
    assert_ne!(info1, info2);
    assert_ne!(info1, info1_t);

    // -- get the result -- //

    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    let mut res: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();

    assert_eq!(1, res.len());
    let one = res.pop().unwrap();
    assert_eq!(one.agent, agent2);
}

#[test]
fn reject_get_no_space() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let addr = format!("http://{:?}/bootstrap", s.listen_addrs()[0]);
    match ureq::get(&addr).call() {
        Err(ureq::Error::StatusCode(status)) => {
            println!("status: {status}");

            //assert!(err.to_string().contains("InvalidPathSegment"));
        }
        oth => panic!("unexpected {oth:?}"),
    }
}

#[test]
fn reject_put_no_space() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let addr = format!("http://{:?}/bootstrap", s.listen_addrs()[0]);
    match ureq::put(&addr).send_empty() {
        Err(ureq::Error::StatusCode(status)) => {
            println!("status: {status}");

            //assert!(err.to_string().contains("InvalidPathSegment"));
        }
        oth => panic!("unexpected {oth:?}"),
    }
}

#[test]
fn reject_put_no_agent() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let addr = format!("http://{:?}/bootstrap/{}", s.listen_addrs()[0], S1);
    match ureq::put(&addr).send_empty() {
        Err(ureq::Error::StatusCode(status)) => {
            println!("status: {status}");

            //assert!(err.to_string().contains("InvalidPathSegment"));
        }
        oth => panic!("unexpected {oth:?}"),
    }
}

#[test]
fn reject_mismatch_agent_url() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        agent_url: Some("AAAA"),
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidAgent"));
}

#[test]
fn reject_mismatch_space_url() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        space_url: "AAAA",
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidSpace"), "Got: {err:?}");
}

#[test]
fn reject_msg_too_long() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let long = String::from_utf8(vec![b's'; 1024]).unwrap();

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        test_prop: &long,
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(
        err.to_string().contains("length limit exceeded"),
        "Got: {err:?}",
    );
}

#[test]
fn reject_old_created_at() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        created_at: 0,
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidCreatedAt"));
}

#[test]
fn reject_future_created_at() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        created_at: i64::MAX - 500,
        expires_at: i64::MAX,
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidCreatedAt"));
}

#[test]
fn reject_expired() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let expires_at = crate::now() - 500;
    let created_at = crate::now() - 1500;

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        created_at,
        expires_at,
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidExpiresAt"));
}

#[test]
fn reject_expired_at_before_created_at() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let expires_at = crate::now() + 500;
    let created_at = crate::now() + 1500;

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        created_at,
        expires_at,
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidExpiresAt"));
}

#[test]
fn reject_expired_at_equals_created_at() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let expires_at = crate::now() + 500;
    let created_at = expires_at;

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        created_at,
        expires_at,
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidExpiresAt"));
}

#[test]
fn reject_expired_at_too_long() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let created_at = crate::now();
    let expires_at =
        created_at + std::time::Duration::from_secs(60 * 40).as_micros() as i64;

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        created_at,
        expires_at,
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidExpiresAt"));
}

#[test]
fn reject_bad_sig() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        signature: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidSignature"));
}

#[test]
fn reject_bad_agent_pub_key() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let err = PutInfo {
        addr: s.listen_addrs()[0],
        // only 31 characters... and obviously the wrong key : )
        final_agent_pk: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ..Default::default()
    }
    .call()
    .unwrap_err();

    assert!(err.to_string().contains("InvalidAgentPubKey"));
}

#[test]
fn default_storage_rollover() {
    let s = BootstrapSrv::new(Config::testing()).unwrap();

    let addr = s.listen_addrs()[0];
    let mut test_prop: u32 = 0;
    let mut put_info = move || {
        use base64::prelude::*;
        let mut agent_seed = [0; 32];
        agent_seed[..4].copy_from_slice(&test_prop.to_le_bytes());
        let agent_seed = BASE64_URL_SAFE_NO_PAD.encode(agent_seed);
        PutInfo {
            addr,
            agent_seed: &agent_seed,
            test_prop: &format!("{test_prop}"),
            ..Default::default()
        }
        .call()
        .unwrap();
        test_prop += 1;
    };

    let addr = s.listen_addrs()[0];
    let get = move || {
        let addr = format!("http://{:?}/bootstrap/{}", addr, S1);
        let res = ureq::get(&addr)
            .call()
            .unwrap()
            .into_body()
            .read_to_string()
            .unwrap();
        let res: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();
        res.into_iter().map(|m| m.test_prop).collect::<Vec<_>>()
    };

    for _ in 0..32 {
        put_info();
    }

    let res = get();

    assert_eq!(
        res.as_slice(),
        &[
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
            "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23",
            "24", "25", "26", "27", "28", "29", "30", "31",
        ]
    );

    for _ in 0..32 {
        put_info();
    }

    let res = get();

    // Agents put beyond MAX_STORAGE will be cause the item at
    // MAX_STORAGE  / 2 to be deleted, then the new put will be appended
    // to the end. The deleted item here is 32 / 2 = index 16, so index 17
    // onward will have the higher test_prop values as they rolled over.
    assert_eq!(
        res.as_slice(),
        &[
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
            "13", "14", "15", "48", "49", "50", "51", "52", "53", "54", "55",
            "56", "57", "58", "59", "60", "61", "62", "63",
        ]
    );
}

#[test]
fn multi_thread_stress() {
    let config = Config::testing();
    let worker_count = config.worker_thread_count as u32;

    let s = BootstrapSrv::new(config).unwrap();
    let addr = s.listen_addrs()[0];

    let start = std::time::Instant::now();

    // the testing config has a small number worker threads (currently 2).
    // Read and write with more than that.
    let t_w_count = worker_count * 8;
    let t_r_count = worker_count * 16;

    // setup readers to just read as fast as possible,
    // then end after the writers are done.

    let w_done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut all_r = Vec::with_capacity(t_r_count as usize);

    for _ in 0..t_r_count {
        let w_done = w_done.clone();

        all_r.push(std::thread::spawn(move || {
            while !w_done.load(std::sync::atomic::Ordering::SeqCst) {
                let addr = format!("http://{:?}/bootstrap/{}", addr, S1);
                let res = ureq::get(&addr)
                    .call()
                    .unwrap()
                    .into_body()
                    .read_to_string()
                    .unwrap();
                let _: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();
            }
        }));
    }

    // update the infos 8 times
    const SCOUNT: u32 = 8;

    let mut all_w = Vec::with_capacity(t_w_count as usize);

    let b = std::sync::Arc::new(std::sync::Barrier::new(t_w_count as usize));

    for a in 0..t_w_count {
        use base64::prelude::*;

        let mut agent_seed = [0; 32];
        agent_seed[..4].copy_from_slice(&a.to_le_bytes());
        let agent_seed = BASE64_URL_SAFE_NO_PAD.encode(agent_seed);

        let b = b.clone();

        all_w.push(std::thread::spawn(move || {
            for i in 0..SCOUNT {
                b.wait();

                PutInfo {
                    addr,
                    agent_seed: &agent_seed,
                    test_prop: &format!("{i}"),
                    ..Default::default()
                }
                .call()
                .unwrap();
            }
        }));
    }

    for j in all_w {
        j.join().unwrap();
    }

    w_done.store(true, std::sync::atomic::Ordering::SeqCst);

    for j in all_r {
        j.join().unwrap();
    }

    let addr = format!("http://{:?}/bootstrap/{}", addr, S1);
    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    let res: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();
    let res = res.into_iter().map(|m| m.test_prop).collect::<Vec<_>>();

    assert_eq!(
        &[
            "7", "7", "7", "7", "7", "7", "7", "7", "7", "7", "7", "7", "7",
            "7", "7", "7"
        ],
        res.as_slice()
    );

    println!("multi_thread_stress in {}s", start.elapsed().as_secs_f64());
}

#[test]
fn expiration_prune() {
    let s = BootstrapSrv::new(Config {
        prune_interval: std::time::Duration::from_millis(5),
        ..Config::testing()
    })
    .unwrap();
    let addr = s.listen_addrs()[0];
    let addr = format!("http://{:?}/bootstrap/{}", addr, S1);

    // -- the entry that WILL get pruned -- //

    let created_at = crate::now();
    let expires_at =
        created_at + std::time::Duration::from_millis(500).as_micros() as i64;

    let _ = PutInfo {
        addr: s.listen_addrs()[0],
        created_at,
        expires_at,
        ..Default::default()
    }
    .call()
    .unwrap();

    // -- the entry that WILL NOT get pruned -- //

    let created_at = crate::now();
    let expires_at =
        created_at + std::time::Duration::from_secs(60).as_micros() as i64;

    let _ = PutInfo {
        addr: s.listen_addrs()[0],
        agent_seed: K2,
        created_at,
        expires_at,
        ..Default::default()
    }
    .call()
    .unwrap();

    /*
     * NOTE - we might be tempted to check that both entries got in there:
     *
     * ```
     * let res = ureq::get(&addr).call().unwrap().into_string().unwrap();
     * let res: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();
     * assert_eq!(2, res.len());
     * ```
     *
     * but the PUTs didn't error, and we have other tests for that.
     *
     * Windows is slow enough that the get will be flaky... sometimes the
     * pruner will have already run on it.
     */

    std::thread::sleep(std::time::Duration::from_secs(1));

    let res = ureq::get(&addr)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    let res: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();

    assert_eq!(1, res.len());
}

#[test]
fn start_with_tls() {
    // We have mixed features between ring and aws_lc so the "lookup by crate features" doesn't
    // return a default.
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    let cert =
        rcgen::generate_simple_self_signed(vec!["bootstrap.test".to_string()])
            .unwrap();
    cert.cert.pem();

    let cert_dir = tempfile::tempdir().unwrap();

    let cert_path = cert_dir.path().join("test_cert.pem");
    File::create_new(&cert_path)
        .unwrap()
        .write_all(cert.cert.pem().as_bytes())
        .unwrap();

    let key_path = cert_dir.path().join("test_key.pem");
    File::create_new(&key_path)
        .unwrap()
        .write_all(cert.key_pair.serialize_pem().as_bytes())
        .unwrap();

    let mut config = Config::testing();
    config.tls_cert = Some(cert_path);
    config.tls_key = Some(key_path);

    let s = BootstrapSrv::new(Config {
        prune_interval: std::time::Duration::from_millis(5),
        ..config
    })
    .unwrap();

    // If we can do a PUT, the server is up and running.
    PutInfo {
        addr: s.listen_addrs()[0],
        use_tls: true,
        ..Default::default()
    }
    .call()
    .unwrap();
}

#[test]
fn use_bootstrap_and_sbd() {
    let s = BootstrapSrv::new(Config {
        prune_interval: std::time::Duration::from_millis(5),
        ..Config::testing()
    })
    .unwrap();
    let addr = s.listen_addrs()[0];

    PutInfo {
        addr: s.listen_addrs()[0],
        ..Default::default()
    }
    .call()
    .unwrap();

    let bootstrap_url = format!("http://{:?}/bootstrap/{}", addr, S1);
    let res = ureq::get(&bootstrap_url)
        .call()
        .unwrap()
        .into_body()
        .read_to_string()
        .unwrap();
    let res: Vec<DecodeAgent> = serde_json::from_str(&res).unwrap();
    assert_eq!(1, res.len());

    async fn connect(
        addr: std::net::SocketAddr,
    ) -> (sbd_client::SbdClient, sbd_client::MsgRecv) {
        sbd_client::SbdClient::connect_config(
            &format!("ws://{addr}"),
            &sbd_client::DefaultCrypto::default(),
            sbd_client::SbdClientConfig {
                allow_plain_text: true,
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (c1, _r1) = connect(addr).await;
        let p1 = c1.pub_key().clone();
        let (c2, mut r2) = connect(addr).await;
        let p2 = c2.pub_key().clone();

        c1.send(&p2, b"hello").await.unwrap();
        let received = r2.recv().await.unwrap().0;
        assert_eq!(p1.as_slice(), &received[0..32]);
        assert_eq!(b"hello".as_slice(), &received[32..]);

        c1.close().await;
        c2.close().await;
    });
}
