use bootstrap_model::BootModel;
use polestar::{
    diagram::write_dot,
    traversal::{traverse, TraversalConfig, TraversalGraphingConfig},
};

fn main() {
    let model = BootModel { initial_workers: 1 };
    let initial = model.initial();
    let config = TraversalConfig::builder()
        .max_depth(1)
        .graphing(TraversalGraphingConfig {
            ignore_loopbacks: true,
        })
        .build();

    let (report, graph, _) =
        traverse(model.into(), initial, config, Some).unwrap();
    let graph = graph.unwrap();
    // let graph = graph.map(
    //     |_, n| n,
    //     |_, e| match e {
    //         BootAction::Update { info, .. } => "Update".to_string(),
    //         e => e.to_string(),
    //     },
    // );

    dbg!(report);
    dbg!(graph.node_count());
    dbg!(graph.edge_count());

    write_dot("bootstrap.dot", &graph, &[]);

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
