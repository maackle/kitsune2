use bootstrap_model::BootModel;
use polestar::diagram::exhaustive::*;

fn main() {
    let model = BootModel { initial_workers: 2 };
    let initial = model.initial();

    let config = DiagramConfig {
        max_depth: Some(1),
        ..Default::default()
    };

    write_dot_state_diagram_mapped(
        "bootstrap.dot",
        model,
        initial,
        &config,
        Some,
        Some,
    );
}
