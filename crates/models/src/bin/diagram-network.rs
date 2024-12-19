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

    OpId::set_limit(1);
    AgentId::set_limit(3);
    DelayMax::set_limit(1);

    let model = NetworkModel::default();
    let initial = model.initial();
    let config = TraversalConfig::builder()
        // .graphing(TraversalGraphingConfig {
        //     ignore_loopbacks: true,
        // })
        .trace_every(100_000)
        // .max_depth(2)
        // .trace_error(true)
        .build();

    let (report, graph, _) =
        traverse(model.into(), initial, config, Some).unwrap();

    dbg!(report);

    if let Some(graph) = graph {
        let graph = graph.map(
            |_, n| {
                n.nodes
                    .iter()
                    .map(|(i, n)| format!("{i}: {} ops", n.sub.ops.len()))
                    .join("\n")
            },
            |_, e| e,
        );
        dbg!(graph.node_count());
        dbg!(graph.edge_count());

        write_dot("out.dot", &graph, &[]);
    }
}
