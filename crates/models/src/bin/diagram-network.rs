use itertools::Itertools;
use kitsune2_models::{
    network::{DelayMax, NetworkModel},
    AgentId, OpId,
};
use polestar::{
    diagram::write_dot,
    traversal::{traverse, TraversalConfig, TraversalGraphingConfig},
};

fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // ops=1, agents=4, delaymax=2:
    // &report = TraversalReport {
    //     num_visited: 566056,
    //     num_terminations: 0,
    //     num_edges_skipped: 24816352,
    //     total_steps: 6882785,
    //     max_depth: 20,
    //     time_taken: 231.498187791s,
    // }

    OpId::set_limit(1);
    AgentId::set_limit(3);
    DelayMax::set_limit(3);

    let model = NetworkModel::default();
    let initial = model.initial();
    let config = TraversalConfig::builder()
        .graphing(TraversalGraphingConfig {
            ignore_loopbacks: true,
        })
        .trace_every(100_000)
        // .max_depth(2)
        // .trace_error(true)
        .build();

    let (report, graph, _) =
        traverse(model.into(), initial, config, Some).unwrap();

    dbg!(&report);

    if let Some(graph) = graph {
        if report.num_visited < 1500 {
            let graph = graph.map(
                |_, n| {
                    let mut lines = n
                        .nodes
                        .iter()
                        .map(|(i, n)| {
                            format!(
                                "{i}: ops [{}] inflight [{}]",
                                n.sub
                                    .ops
                                    .iter()
                                    .map(ToString::to_string)
                                    .join(" "),
                                n.inflight
                                    .iter()
                                    .map(ToString::to_string)
                                    .join(" ")
                            )
                        })
                        .collect_vec();
                    lines.sort();
                    lines.push("".to_string());
                    lines.join("\n")
                },
                |_, e| e,
            );
            dbg!(graph.node_count());
            dbg!(graph.edge_count());

            write_dot("out.dot", &graph, &[]);
            println!("wrote DOt graph");
        }
    }
}
