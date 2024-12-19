use kitsune2_models::{
    network::{DelayMax, NetworkModel},
    AgentId, OpId,
};
use polestar::{
    diagram::write_dot,
    traversal::{traverse, TraversalConfig, TraversalGraphingConfig},
};

fn main() {
    OpId::set_limit(1);
    AgentId::set_limit(3);
    DelayMax::set_limit(1);

    let model = NetworkModel::default();
    let initial = model.initial();
    let config = TraversalConfig::builder()
        // .max_depth(4)
        .graphing(TraversalGraphingConfig {
            ignore_loopbacks: true,
        })
        .build();

    let (report, graph, _) =
        traverse(model.into(), initial, config, Some).unwrap();
    let graph = graph.unwrap();
    let graph = graph.map(|_, n| format!("{:?}", n.sub.ops), |_, e| e);

    dbg!(report);
    dbg!(graph.node_count());
    dbg!(graph.edge_count());

    write_dot("out.dot", &graph, &[]);

    // let config = DiagramConfig {
    //     max_depth: Some(1),
    //     ..Default::default()
    // };

    // write_dot_state_diagram_mapped(
    //     "bootstrap.dot",
    //     model,
    //     initial,
    //     &config,
    //     Some,
    //     Some,
    // );
}
