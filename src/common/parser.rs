use anyhow::{anyhow, Context, Result};
use core::fmt::Formatter;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};

#[derive(Debug, Clone)]
pub struct Parser<'a> {
    path: Vec<&'a str>,
    node: roxmltree::Node<'a, 'a>,
}

impl<'a> Parser<'a> {
    pub fn context(&self) -> String {
        //format!("{:?}: {:?}", self.path, self.node)
        format!("{:?}", self.path)
    }
}
impl<'a> Display for Parser<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <Parser as Debug>::fmt(self, f)
    }
}

impl<'a> Parser<'a> {
    pub fn node(&self) -> roxmltree::Node<'a, 'a> {
        self.node
    }
    pub fn children_names(&self) -> Vec<&'a str> {
        self.node
            .children()
            .filter_map(|c| {
                if c.is_element() {
                    Some(c.tag_name().name())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
    pub fn new(node: roxmltree::Node<'a, 'a>) -> Self {
        Self { node, path: vec![] }
    }
    pub fn named(self, name: &str) -> Result<Self> {
        let other = self.node.tag_name().name();
        if other == name {
            Ok(self)
        } else {
            return Err(anyhow!("expected element name '{name}', found '{other}'"))
                .context(self.context());
        }
    }

    pub fn attribute(&self, name: &str) -> Result<&'a str> {
        let r = self
            .node
            .attribute_node(name)
            .ok_or(anyhow!("expected attribute '{name}'"))
            .map(|a| a.value())
            .context(self.context())?;
        Ok(r)
    }

    pub fn has_elements(&self) -> bool {
        self.node.children().any(|c| c.is_element())
    }

    pub fn only_same_named_children1(self) -> Result<Vec<Parser<'a>>> {
        let out = self.clone().only_same_named_children()?;
        if out.is_empty() {
            Err(anyhow!("no children")).context(self.context())
        } else {
            Ok(out)
        }
    }

    pub fn only_same_named_children(self) -> Result<Vec<Parser<'a>>> {
        let mut out: Vec<Parser<'a>> = vec![];
        let _iter = self.node.children();
        for el in self.node.children() {
            if !el.is_element() {
                continue;
            }
            let name = el.tag_name().name();
            if let Some(single_name) = out.first().map(|n| n.node.tag_name().name()) {
                if name != single_name {
                    return Err(anyhow!("there are elements named '{name}' and '{single_name}' where single name is expected"))
                        .context(self.context());
                }
            }
            let mut path = self.path.clone();
            path.push(name);
            out.push(Parser { path, node: el });
        }
        Ok(out)
    }

    pub fn uniquely_named_children_map(self) -> Result<HashMap<String, Parser<'a>>> {
        let mut out = HashMap::new();
        for el in self.node.children() {
            if !el.is_element() {
                continue;
            }
            let name = el.tag_name().name();
            let mut path = self.path.clone();
            path.push(name);
            if out
                .insert(name.to_string(), Self { node: el, path })
                .is_some()
            {
                return Err(anyhow!("there are more than one elements named '{name}'"))
                    .context(self.context());
            }
        }
        Ok(out)
    }

    pub fn uniquely_named_children(self) -> Result<Vec<Parser<'a>>> {
        let mut out = vec![];
        let mut seen = HashSet::new();
        for el in self.node.children() {
            if !el.is_element() {
                continue;
            }
            let name = el.tag_name().name();
            if !seen.insert(name) {
                return Err(anyhow!("there are more than one elements named '{name}'"))
                    .context(self.context());
            }
            let mut path = self.path.clone();
            path.push(name);
            out.push(Parser { path, node: el });
        }
        Ok(out)
    }

    pub fn uniquely_named_children1(self) -> Result<Vec<Parser<'a>>> {
        let out = self.clone().uniquely_named_children()?;
        if out.is_empty() {
            Err(anyhow!("no children")).context(self.context())
        } else {
            Ok(out)
        }
    }

    // pub fn uniquely_named_child(self, name: &str) -> Result<Self> {
    //     self.uniquely_named_child_opt(name)?
    //         .ok_or(anyhow!("expected single child named '{name}', found none"))
    // }

    pub fn uniquely_named_child_opt(self, name: &str) -> Result<Option<Self>> {
        let children = self
            .node
            .children()
            .filter(|c| c.is_element() && c.tag_name().name() == name)
            .collect::<Vec<_>>();
        if children.len() > 1 {
            return Err(anyhow!(
                "expected single child named '{name}', found {}",
                children.len(),
            ))
            .context(self.context());
        }
        if let Some(child) = children.first() {
            let mut path = self.path.clone();
            path.push(children[0].tag_name().name());
            Ok(Some(Self { path, node: *child }))
        } else {
            Ok(None)
        }
    }

    pub fn single_child(self) -> Result<Self> {
        let children = self
            .node
            .children()
            .filter(|c| c.is_element())
            .collect::<Vec<_>>();
        if children.len() != 1 {
            return Err(anyhow!(
                "expected single child, found {}{}: {:?}",
                children.len(),
                if children.len() > 10 {
                    " (showing first 10)"
                } else {
                    ""
                },
                children
                    .iter()
                    .map(|c| c.tag_name().name())
                    .take(10)
                    .collect::<Vec<_>>()
            ))
            .context(self.context());
        }
        let mut path = self.path.clone();
        path.push(children[0].tag_name().name());
        Ok(Self {
            path,
            node: children[0],
        })
    }
}
