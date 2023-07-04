use std::io;

use broadcast::{init, Node};

fn main() -> io::Result<()> {
    let (id, node_ids) = init()?;

    let mut node = Node::new(id, node_ids, 3);
    node.run()?;

    Ok(())
}
