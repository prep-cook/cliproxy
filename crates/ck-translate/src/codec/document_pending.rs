use super::*;

/// Native topology assembled once; open strings are never exposed as completed values.
pub(crate) struct Pending {
    pub document: Document,
    pub scanner: json::Scanner,
    nodes: super::super::table::Table<usize>,
    pub string: Option<usize>,
}

impl Pending {
    pub fn new(owner: &mut Writer, limit: u64) -> Result<Self> {
        Ok(Self {
            document: Document::streaming(owner)?,
            scanner: json::Scanner::new(owner, limit)?,
            nodes: super::super::table::Table::new(owner)?,
            string: None,
        })
    }
    pub fn node(&self, wire: usize) -> Result<usize> {
        self.nodes.get(wire).copied().ok_or(Error::Position)
    }
    pub fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        self.document.push_input(owner, bytes)?;
        self.scanner.advance_document(&self.document)?;
        self.sync()
    }
    pub fn finish(&mut self) -> Result<()> {
        self.scanner.finish(self.document.input())?;
        self.sync()
    }
    fn sync(&mut self) -> Result<()> {
        if let Some(wire) = self.string {
            let scanned = self.scanner.pending_node(self.document.input(), wire)?;
            if scanned.span.end != 0 {
                let node = self.node(wire)?;
                if matches!(self.document.node(node)?.value, Value::String(_)) {
                    self.document.nodes[node].value = Value::String(Text {
                        source: 0,
                        span: scanned.span,
                    });
                }
                self.string = None;
            }
        }
        while self.nodes.len() < self.scanner.nodes().len() {
            let wire = self.nodes.len();
            let scanned = self.scanner.pending_node(self.document.input(), wire)?;
            if wire == 0 {
                if scanned.kind != json::Kind::Object {
                    return Err(Error::Schema);
                }
                self.nodes.push(self.document.root())?;
                continue;
            }
            let text = Text {
                source: 0,
                span: scanned.span,
            };
            let value = match scanned.kind {
                json::Kind::Object => Value::Object,
                json::Kind::Array => Value::Array,
                json::Kind::String => Value::String(text),
                json::Kind::Number => Value::Number(text),
                json::Kind::Bool => Value::Bool(self.document.input().equals(scanned.span, b"true")?),
                json::Kind::Null => Value::Null,
            };
            let node = self
                .document
                .add(scanned.key.map(|span| Name::Mapped(Text { source: 0, span })), value)?;
            self.document
                .append(self.node(scanned.parent.ok_or(Error::Schema)?)?, node)?;
            self.nodes.push(node)?;
            if scanned.kind == json::Kind::String && scanned.span.end == 0 {
                self.string = Some(wire);
            }
        }
        Ok(())
    }
}
