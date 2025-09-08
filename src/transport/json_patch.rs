#[derive(thiserror::Error, Debug)]
pub enum PatchLoroDocumentError {
    #[error("failed to undo patches (this should never happen): {0}")]
    UndoPatchesError(PatchError),
    #[error("failed to apply patches: {0}")]
    ApplyPatchesError(PatchError),
}

/// This type represents all possible errors that can occur when applying JSON patch
#[derive(thiserror::Error, Debug)]
pub enum PatchErrorKind {
    #[error("value did not match")]
    TestFailed,
    #[error("path is invalid: out of bounds: {0}")]
    InvalidPointerOOB(String),
    #[error("path is invalid: trying to index with non-index: {0}")]
    InvalidPointerNonIndex(String),
    #[error("path is invalid: not found: {0}")]
    InvalidPointerNotFound(String),
    #[error("cannot move the value inside itself")]
    CannotMoveInsideItself,
    #[error("cannot add a value to the root of the document without being a crdt value")]
    CannotAddToRootWithoutCrdtValue,
    #[error("cannot convert JSON value to Loro value: {0}")]
    CannotConvertJsonToLoro(#[from] super::serde::SerdeToLoroError),
    #[error("invalid value for this operation: {0}")]
    InvalidValue(String),
    #[error("cannot add a child to a non-container")]
    CannotAddChildToNonContainer,
    #[error("cannot add a container to a non-container")]
    CannotAddContainerToNonContainer,
    #[error("copy operation must only copy a value, not a container")]
    CopyMustOnlyCopyValue,
    #[error("test operation must only test a value, not a container")]
    TestMustOnlyTestValue,
    #[error("unknown container")]
    UnknownContainer,
    #[error("loro error: {0}")]
    LoroError(#[from] loro::LoroError),
}

fn safe_get_by_str_path(
    doc: std::sync::Arc<loro::LoroDoc>,
    path: &jsonptr::Pointer,
) -> Option<loro::ValueOrContainer> {
    let s = &path.to_string();
    if s.chars().next() == Some('/') {
        let mut chars = s.chars();
        chars.next();
        doc.get_by_str_path(chars.as_str())
    } else {
        doc.get_by_str_path(s)
    }
}

/// This type represents all possible errors that can occur when applying JSON patch
#[derive(thiserror::Error, Debug)]
#[error("operation '/{operation}' failed at path '{path}': {kind}")]
pub struct PatchError {
    /// Index of the operation that has failed.
    pub operation: usize,
    /// `path` of the operation.
    pub path: jsonptr::PointerBuf,
    /// Kind of the error.
    pub kind: PatchErrorKind,
}

fn translate_error(kind: PatchErrorKind, operation: usize, path: &jsonptr::Pointer) -> PatchError {
    PatchError {
        operation,
        path: path.to_owned(),
        kind,
    }
}

/// Representation of JSON Patch (list of patch operations)
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct Patch(pub Vec<PatchOperation>);

impl std::ops::Deref for Patch {
    type Target = [PatchOperation];

    fn deref(&self) -> &[PatchOperation] {
        &self.0
    }
}

/// JSON Patch 'add' operation representation
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct AddOperation {
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// within the target document where the operation is performed.
    pub path: jsonptr::PointerBuf,
    /// Value to add to the target location.
    pub value: serde_json::Value,
}

/// JSON Patch 'remove' operation representation
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct RemoveOperation {
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// within the target document where the operation is performed.
    pub path: jsonptr::PointerBuf,
}

/// JSON Patch 'replace' operation representation
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct ReplaceOperation {
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// within the target document where the operation is performed.
    pub path: jsonptr::PointerBuf,
    /// Value to replace with.
    pub value: serde_json::Value,
}

/// JSON Patch 'move' operation representation
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct MoveOperation {
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// to move value from.
    pub from: jsonptr::PointerBuf,
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// within the target document where the operation is performed.
    pub path: jsonptr::PointerBuf,
}

/// JSON Patch 'copy' operation representation
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct CopyOperation {
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// to copy value from.
    pub from: jsonptr::PointerBuf,
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// within the target document where the operation is performed.
    pub path: jsonptr::PointerBuf,
}

/// JSON Patch 'test' operation representation
#[derive(Clone, Debug, Default, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct TestOperation {
    /// JSON-Pointer value [RFC6901](https://tools.ietf.org/html/rfc6901) that references a location
    /// within the target document where the operation is performed.
    pub path: jsonptr::PointerBuf,
    /// Value to test against.
    pub value: serde_json::Value,
}

/// JSON Patch single patch operation
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "op")]
#[serde(rename_all = "lowercase")]
pub enum PatchOperation {
    /// 'add' operation
    Add(AddOperation),
    /// 'remove' operation
    Remove(RemoveOperation),
    /// 'replace' operation
    Replace(ReplaceOperation),
    /// 'move' operation
    Move(MoveOperation),
    /// 'copy' operation
    Copy(CopyOperation),
    /// 'test' operation
    Test(TestOperation),
}

impl Default for PatchOperation {
    fn default() -> Self {
        PatchOperation::Test(TestOperation::default())
    }
}

struct AddUndoOperation {
    path: jsonptr::PointerBuf,
    value: loro::ValueOrContainer,
}

struct RemoveUndoOperation {
    path: jsonptr::PointerBuf,
}

struct ReplaceUndoOperation {
    path: jsonptr::PointerBuf,
    value: loro::ValueOrContainer,
}

struct MoveUndoOperation {
    from: jsonptr::PointerBuf,
    path: jsonptr::PointerBuf,
}

enum UndoOperation {
    Add(AddUndoOperation),
    Remove(RemoveUndoOperation),
    Replace(ReplaceUndoOperation),
    Move(MoveUndoOperation),
}

fn add_rec(
    doc: std::sync::Arc<loro::LoroDoc>,
    parent: &mut loro::ValueOrContainer,
    path: &jsonptr::Pointer,
    value: loro::ValueOrContainer,
    undo_stack: Option<&mut Vec<UndoOperation>>,
    full_path: &jsonptr::PointerBuf,
) -> Result<Option<loro::ValueOrContainer>, PatchErrorKind> {
    let Some((first, rest)) = path.split_front() else {
        unreachable!("Path should not be empty here");
    };
    match parent {
        loro::ValueOrContainer::Container(container) => match container {
            loro::Container::List(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                if rest.is_empty() {
                    let Ok(idx) = idx.for_len_incl(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            l.insert(idx, v)?;
                        }
                        loro::ValueOrContainer::Container(c) => {
                            l.insert_container(idx, c)?;
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
                            path: path.to_buf(),
                        }))
                    }
                } else {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let Some(mut val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    if let Some(v) = add_rec(doc, &mut val, rest, value, undo_stack, full_path)? {
                        l.delete(idx, 1)?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                l.insert(idx, v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                l.insert_container(idx, c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Map(m) => {
                let val = m.get(&first.decoded());
                if rest.is_empty() {
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            m.insert(&first.decoded(), v)?;
                        }
                        loro::ValueOrContainer::Container(c) => {
                            m.insert_container(&first.decoded(), c)?;
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        if let Some(old) = val {
                            undo_stack.push(UndoOperation::Add(AddUndoOperation {
                                path: path.to_buf(),
                                value: old,
                            }))
                        } else {
                            undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
                                path: path.to_buf(),
                            }))
                        }
                    }
                } else {
                    let Some(val) = val else {
                        return Err(PatchErrorKind::InvalidPointerNotFound(first.to_string()));
                    };
                    if let Some(v) =
                        add_rec(doc, &mut val.clone(), rest, value, undo_stack, full_path)?
                    {
                        m.delete(&first.decoded())?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                m.insert(&first.decoded(), v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                m.insert_container(&first.decoded(), c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Text(t) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                if rest.is_empty() {
                    let Ok(idx) = idx.for_len_incl(t.len_utf8()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let str_len;
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            if let loro::LoroValue::String(s) = v {
                                str_len = s.len();
                                t.insert_utf8(idx, &*s)?;
                            } else {
                                return Err(PatchErrorKind::InvalidValue(
                                    "expected string value for text container".to_string(),
                                ));
                            }
                        }
                        loro::ValueOrContainer::Container(_) => {
                            return Err(PatchErrorKind::InvalidValue(
                                "expected string value for text container, got container"
                                    .to_string(),
                            ));
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        for _ in 0..str_len {
                            undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
                                path: path.to_buf(),
                            }))
                        }
                    }
                } else {
                    return Err(PatchErrorKind::CannotAddChildToNonContainer);
                }
                return Ok(None);
            }
            loro::Container::Tree(_) => {
                unimplemented!("Adding to a tree is not implemented yet");
            }
            loro::Container::MovableList(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                if rest.is_empty() {
                    let Ok(idx) = idx.for_len_incl(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            l.insert(idx, v)?;
                        }
                        loro::ValueOrContainer::Container(c) => {
                            l.insert_container(idx, c)?;
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
                            path: path.to_buf(),
                        }))
                    }
                } else {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let Some(mut val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    if let Some(v) = add_rec(doc, &mut val, rest, value, undo_stack, full_path)? {
                        l.delete(idx, 1)?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                l.insert(idx, v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                l.insert_container(idx, c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Counter(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::Container::Unknown(_) => {
                return Err(PatchErrorKind::UnknownContainer);
            }
        },
        loro::ValueOrContainer::Value(v) => match v {
            loro::LoroValue::Null => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Bool(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Double(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::I64(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Binary(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::String(s) => {
                if rest.len() > 0 {
                    return Err(PatchErrorKind::CannotAddChildToNonContainer);
                }
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let Ok(idx) = idx.for_len_incl(s.len()) else {
                    return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                };
                let str_len;
                let new_value = match value {
                    loro::ValueOrContainer::Value(v) => {
                        if let loro::LoroValue::String(vs) = v {
                            str_len = vs.len();
                            let mut s = (**s).clone();
                            s.insert_str(idx, &*vs);
                            s
                        } else {
                            return Err(PatchErrorKind::InvalidValue(
                                "expected string value for text container".to_string(),
                            ));
                        }
                    }
                    loro::ValueOrContainer::Container(_) => {
                        return Err(PatchErrorKind::InvalidValue(
                            "expected string value for text container, got container".to_string(),
                        ));
                    }
                };
                if let Some(&mut ref mut undo_stack) = undo_stack {
                    for _ in 0..str_len {
                        undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
                            path: path.to_buf(),
                        }))
                    }
                }
                Ok(Some(loro::ValueOrContainer::Value(
                    loro::LoroValue::String(new_value.into()),
                )))
            }
            loro::LoroValue::List(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let Ok(idx) = idx.for_len_incl(l.len()) else {
                    return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                };
                let mut new_list = (**l).clone();

                let new_value;
                if rest.is_empty() {
                    new_value = match value {
                        loro::ValueOrContainer::Value(v) => {
                            new_list.insert(idx, v);
                            loro::ValueOrContainer::Value(loro::LoroValue::List(new_list.into()))
                        }
                        loro::ValueOrContainer::Container(_) => {
                            return Err(PatchErrorKind::CannotAddContainerToNonContainer);
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
                            path: path.to_buf(),
                        }))
                    }
                } else {
                    let Some(list_el) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let val = add_rec(
                        doc.clone(),
                        &mut loro::ValueOrContainer::Value(list_el.clone()),
                        rest,
                        value,
                        undo_stack,
                        full_path,
                    )?;
                    if let Some(loro::ValueOrContainer::Value(val)) = val {
                        new_list.push(val);
                        new_list.swap_remove(idx);
                        new_value =
                            loro::ValueOrContainer::Value(loro::LoroValue::List(new_list.into()))
                    } else {
                        unreachable!("add_rec should return a ValueOrContainer::Value here");
                    }
                }
                Ok(Some(new_value))
            }
            loro::LoroValue::Map(m) => {
                let old = m.get(&first.decoded().to_string());
                let mut new_map = (**m).clone();

                let new_value;
                if rest.is_empty() {
                    new_value = match value {
                        loro::ValueOrContainer::Value(v) => {
                            new_map.insert(first.decoded().into(), v);
                            loro::ValueOrContainer::Value(loro::LoroValue::Map(new_map.into()))
                        }
                        loro::ValueOrContainer::Container(_) => {
                            return Err(PatchErrorKind::CannotAddContainerToNonContainer);
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        if let Some(old) = old {
                            undo_stack.push(UndoOperation::Add(AddUndoOperation {
                                path: path.to_buf(),
                                value: loro::ValueOrContainer::Value(old.clone()),
                            }))
                        } else {
                            undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
                                path: path.to_buf(),
                            }))
                        }
                    }
                } else {
                    let Some(old) = old else {
                        return Err(PatchErrorKind::InvalidPointerNotFound(
                            first.decoded().to_string(),
                        ));
                    };
                    let val = add_rec(
                        doc.clone(),
                        &mut loro::ValueOrContainer::Value(old.clone()),
                        rest,
                        value,
                        undo_stack,
                        full_path,
                    )?;
                    if let Some(loro::ValueOrContainer::Value(val)) = val {
                        new_map.insert(first.decoded().into(), val);
                        new_value =
                            loro::ValueOrContainer::Value(loro::LoroValue::Map(new_map.into()))
                    } else {
                        if full_path == path {
                            return Ok(None);
                        }
                        unreachable!("add_rec should return a ValueOrContainer::Value here");
                    }
                }
                return Ok(Some(new_value));
            }
            loro::LoroValue::Container(c) => {
                let container_type = match c {
                    loro::ContainerID::Root {
                        name: _,
                        container_type,
                    } => container_type,
                    loro::ContainerID::Normal {
                        peer: _,
                        counter: _,
                        container_type,
                    } => container_type,
                };
                add_rec(
                    doc.clone(),
                    &mut loro::ValueOrContainer::Container(match container_type {
                        loro::ContainerType::Text => loro::Container::Text(doc.get_text(c.clone())),
                        loro::ContainerType::Map => loro::Container::Map(doc.get_map(c.clone())),
                        loro::ContainerType::List => loro::Container::List(doc.get_list(c.clone())),
                        loro::ContainerType::MovableList => {
                            loro::Container::MovableList(doc.get_movable_list(c.clone()))
                        }
                        loro::ContainerType::Tree => loro::Container::Tree(doc.get_tree(c.clone())),
                        loro::ContainerType::Counter => {
                            loro::Container::Counter(doc.get_counter(c.clone()))
                        }
                        loro::ContainerType::Unknown(_) => {
                            return Err(PatchErrorKind::UnknownContainer);
                        }
                    }),
                    path,
                    value,
                    undo_stack,
                    full_path,
                )
            }
        },
    }
}

fn add(
    doc: std::sync::Arc<loro::LoroDoc>,
    path: &jsonptr::Pointer,
    value: serde_json::Value,
    undo_stack: Option<&mut Vec<UndoOperation>>,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() {
        let _root_value = super::serde::serde_to_loro_root(value, Some(doc.clone()))
            .map_err(PatchErrorKind::CannotConvertJsonToLoro)?;
        // TODO: implement root add undo
        // if let Some(&mut ref mut undo_stack) = undo_stack {
        //     undo_stack.push(UndoOperation::Remove(RemoveUndoOperation {
        //         path: path.to_buf(),
        //     }))
        // }
        return Ok(());
    };

    let loro_value =
        super::serde::serde_to_loro(value).map_err(PatchErrorKind::CannotConvertJsonToLoro)?;
    let parent = doc.get_value();

    match add_rec(
        doc,
        &mut loro::ValueOrContainer::Value(parent),
        path,
        loro_value,
        undo_stack,
        &path.to_buf(),
    ) {
        Err(e) => Err(e),
        Ok(_) => Ok(()),
    }
}

fn add_loro(
    doc: std::sync::Arc<loro::LoroDoc>,
    path: &jsonptr::Pointer,
    value: loro::ValueOrContainer,
    undo_stack: Option<&mut Vec<UndoOperation>>,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    let parent = doc.get_value();

    match add_rec(
        doc,
        &mut loro::ValueOrContainer::Value(parent),
        path,
        value,
        undo_stack,
        &path.to_buf(),
    ) {
        Err(e) => Err(e),
        Ok(_) => Ok(()),
    }
}

fn remove_rec(
    doc: std::sync::Arc<loro::LoroDoc>,
    parent: &mut loro::ValueOrContainer,
    path: &jsonptr::Pointer,
    allow_last: bool,
    undo_stack: Option<&mut Vec<UndoOperation>>,
    full_path: &jsonptr::PointerBuf,
) -> Result<Option<loro::ValueOrContainer>, PatchErrorKind> {
    let Some((first, rest)) = path.split_front() else {
        unreachable!("Path should not be empty here");
    };
    match parent {
        loro::ValueOrContainer::Container(container) => match container {
            loro::Container::List(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let idx = if allow_last && idx == jsonptr::index::Index::Next {
                    if l.len() == 0 {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    }
                    l.len() - 1
                } else {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    idx
                };
                if rest.is_empty() {
                    let Some(old_val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    l.delete(idx, 1)?;
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: old_val,
                        }))
                    }
                } else {
                    let Some(mut val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    if let Some(v) =
                        remove_rec(doc, &mut val, rest, allow_last, undo_stack, full_path)?
                    {
                        l.delete(idx, 1)?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                l.insert(idx, v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                l.insert_container(idx, c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Map(m) => {
                let Some(val) = m.get(&first.decoded()) else {
                    return Err(PatchErrorKind::InvalidPointerNotFound(
                        first.decoded().to_string(),
                    ));
                };
                if rest.is_empty() {
                    m.delete(&first.decoded())?;
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: val,
                        }))
                    }
                } else {
                    if let Some(v) = remove_rec(
                        doc,
                        &mut val.clone(),
                        rest,
                        allow_last,
                        undo_stack,
                        full_path,
                    )? {
                        m.delete(&first.decoded())?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                m.insert(&first.decoded(), v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                m.insert_container(&first.decoded(), c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Text(t) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let idx = if allow_last && idx == jsonptr::index::Index::Next {
                    if t.len_utf8() == 0 {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    }
                    t.len_utf8() - 1
                } else {
                    let Ok(idx) = idx.for_len(t.len_utf8()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    idx
                };
                if rest.is_empty() {
                    let Ok(value) = t.char_at(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    t.delete_utf8(idx, 1)?;
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: loro::ValueOrContainer::Value(loro::LoroValue::String(
                                value.to_string().into(),
                            )),
                        }))
                    }
                } else {
                    return Err(PatchErrorKind::CannotAddChildToNonContainer);
                }
                return Ok(None);
            }
            loro::Container::Tree(_) => {
                unimplemented!("Removing from a tree is not implemented yet");
            }
            loro::Container::MovableList(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let idx = if allow_last && idx == jsonptr::index::Index::Next {
                    if l.len() == 0 {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    }
                    l.len() - 1
                } else {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    idx
                };
                if rest.is_empty() {
                    let Some(old_val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    l.delete(idx, 1)?;
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: old_val,
                        }))
                    }
                } else {
                    let Some(mut val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    if let Some(v) =
                        remove_rec(doc, &mut val, rest, allow_last, undo_stack, full_path)?
                    {
                        l.delete(idx, 1)?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                l.insert(idx, v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                l.insert_container(idx, c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Counter(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::Container::Unknown(_) => {
                return Err(PatchErrorKind::UnknownContainer);
            }
        },
        loro::ValueOrContainer::Value(v) => match v {
            loro::LoroValue::Null => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Bool(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Double(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::I64(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Binary(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::String(s) => {
                if rest.len() > 0 {
                    return Err(PatchErrorKind::CannotAddChildToNonContainer);
                }
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let idx = if allow_last && idx == jsonptr::index::Index::Next {
                    if s.len() == 0 {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    }
                    s.len() - 1
                } else {
                    let Ok(idx) = idx.for_len(s.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    idx
                };
                let mut s = (**s).clone();
                let old = s.remove(idx);
                if let Some(&mut ref mut undo_stack) = undo_stack {
                    undo_stack.push(UndoOperation::Add(AddUndoOperation {
                        path: path.to_buf(),
                        value: loro::ValueOrContainer::Value(loro::LoroValue::String(
                            old.to_string().into(),
                        )),
                    }))
                }
                Ok(Some(loro::ValueOrContainer::Value(
                    loro::LoroValue::String(s.into()),
                )))
            }
            loro::LoroValue::List(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let idx = if allow_last && idx == jsonptr::index::Index::Next {
                    if l.len() == 0 {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    }
                    l.len() - 1
                } else {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    idx
                };
                let mut new_list = (**l).clone();
                let new_value;
                if rest.is_empty() {
                    let old = new_list.remove(idx);
                    new_value =
                        loro::ValueOrContainer::Value(loro::LoroValue::List(new_list.into()));
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: loro::ValueOrContainer::Value(old),
                        }))
                    }
                } else {
                    let Some(list_el) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let val = remove_rec(
                        doc.clone(),
                        &mut loro::ValueOrContainer::Value(list_el.clone()),
                        rest,
                        allow_last,
                        undo_stack,
                        full_path,
                    )?;
                    if let Some(loro::ValueOrContainer::Value(val)) = val {
                        new_list.push(val);
                        new_list.swap_remove(idx);
                        new_value =
                            loro::ValueOrContainer::Value(loro::LoroValue::List(new_list.into()))
                    } else {
                        unreachable!("remove_rec should return a ValueOrContainer::Value here");
                    }
                }
                Ok(Some(new_value))
            }
            loro::LoroValue::Map(m) => {
                let old = m.get(&first.decoded().to_string());
                let mut new_map = (**m).clone();

                let new_value;
                if rest.is_empty() {
                    let Some(old) = new_map.remove(&first.decoded().to_string()) else {
                        return Err(PatchErrorKind::InvalidPointerNotFound(
                            first.decoded().to_string(),
                        ));
                    };
                    new_value = loro::ValueOrContainer::Value(loro::LoroValue::Map(new_map.into()));
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: loro::ValueOrContainer::Value(old.clone()),
                        }))
                    }
                } else {
                    let Some(old) = old else {
                        return Err(PatchErrorKind::InvalidPointerNotFound(
                            first.decoded().to_string(),
                        ));
                    };
                    let val = remove_rec(
                        doc.clone(),
                        &mut loro::ValueOrContainer::Value(old.clone()),
                        rest,
                        allow_last,
                        undo_stack,
                        full_path,
                    )?;
                    if let Some(loro::ValueOrContainer::Value(val)) = val {
                        new_map.insert(first.decoded().into(), val);
                        new_value =
                            loro::ValueOrContainer::Value(loro::LoroValue::Map(new_map.into()))
                    } else {
                        if full_path == path {
                            return Ok(None);
                        }
                        unreachable!("remove_rec should return a ValueOrContainer::Value here");
                    }
                }
                return Ok(Some(new_value));
            }
            loro::LoroValue::Container(c) => {
                let container_type = match c {
                    loro::ContainerID::Root {
                        name: _,
                        container_type,
                    } => container_type,
                    loro::ContainerID::Normal {
                        peer: _,
                        counter: _,
                        container_type,
                    } => container_type,
                };
                remove_rec(
                    doc.clone(),
                    &mut loro::ValueOrContainer::Container(match container_type {
                        loro::ContainerType::Text => loro::Container::Text(doc.get_text(c.clone())),
                        loro::ContainerType::Map => loro::Container::Map(doc.get_map(c.clone())),
                        loro::ContainerType::List => loro::Container::List(doc.get_list(c.clone())),
                        loro::ContainerType::MovableList => {
                            loro::Container::MovableList(doc.get_movable_list(c.clone()))
                        }
                        loro::ContainerType::Tree => loro::Container::Tree(doc.get_tree(c.clone())),
                        loro::ContainerType::Counter => {
                            loro::Container::Counter(doc.get_counter(c.clone()))
                        }
                        loro::ContainerType::Unknown(_) => {
                            return Err(PatchErrorKind::UnknownContainer);
                        }
                    }),
                    path,
                    allow_last,
                    undo_stack,
                    full_path,
                )
            }
        },
    }
}

fn remove(
    doc: std::sync::Arc<loro::LoroDoc>,
    path: &jsonptr::Pointer,
    allow_last: bool,
    undo_stack: Option<&mut Vec<UndoOperation>>,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    let parent = doc.get_value();

    match remove_rec(
        doc,
        &mut loro::ValueOrContainer::Value(parent),
        path,
        allow_last,
        undo_stack,
        &path.to_buf(),
    ) {
        Err(e) => Err(e),
        Ok(_) => Ok(()),
    }
}

fn replace_rec(
    doc: std::sync::Arc<loro::LoroDoc>,
    parent: &mut loro::ValueOrContainer,
    path: &jsonptr::Pointer,
    value: loro::ValueOrContainer,
    undo_stack: Option<&mut Vec<UndoOperation>>,
    full_path: &jsonptr::PointerBuf,
) -> Result<Option<loro::ValueOrContainer>, PatchErrorKind> {
    let Some((first, rest)) = path.split_front() else {
        unreachable!("Path should not be empty here");
    };
    match parent {
        loro::ValueOrContainer::Container(container) => match container {
            loro::Container::List(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                if rest.is_empty() {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let Some(old_val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    l.delete(idx, 1)?;
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            l.insert(idx, v)?;
                        }
                        loro::ValueOrContainer::Container(c) => {
                            l.insert_container(idx, c)?;
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Replace(ReplaceUndoOperation {
                            path: path.to_buf(),
                            value: old_val,
                        }))
                    }
                } else {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let Some(mut val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    if let Some(v) = replace_rec(doc, &mut val, rest, value, undo_stack, full_path)?
                    {
                        l.delete(idx, 1)?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                l.insert(idx, v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                l.insert_container(idx, c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Map(m) => {
                let val = m.get(&first.decoded());
                let Some(val) = val else {
                    return Err(PatchErrorKind::InvalidPointerNotFound(
                        first.decoded().to_string(),
                    ));
                };
                if rest.is_empty() {
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            m.insert(&first.decoded(), v)?;
                        }
                        loro::ValueOrContainer::Container(c) => {
                            m.insert_container(&first.decoded(), c)?;
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: val,
                        }))
                    }
                } else {
                    if let Some(v) =
                        replace_rec(doc, &mut val.clone(), rest, value, undo_stack, full_path)?
                    {
                        m.delete(&first.decoded())?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                m.insert(&first.decoded(), v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                m.insert_container(&first.decoded(), c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Text(t) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                if rest.is_empty() {
                    let Ok(idx) = idx.for_len(t.len_utf8()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let str_len;
                    let old_str;
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            if let loro::LoroValue::String(s) = v {
                                str_len = s.len();
                                old_str = t
                                    .to_string()
                                    .get(idx..idx + str_len)
                                    .ok_or(PatchErrorKind::InvalidPointerOOB(first.to_string()))?
                                    .to_string();
                                t.delete_utf8(idx, s.len())?;
                                t.insert_utf8(idx, &*s)?;
                            } else {
                                return Err(PatchErrorKind::InvalidValue(
                                    "expected string value for text container".to_string(),
                                ));
                            }
                        }
                        loro::ValueOrContainer::Container(_) => {
                            return Err(PatchErrorKind::InvalidValue(
                                "expected string value for text container, got container"
                                    .to_string(),
                            ));
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Replace(ReplaceUndoOperation {
                            path: path.to_buf(),
                            value: loro::ValueOrContainer::Value(loro::LoroValue::String(
                                old_str.to_string().into(),
                            )),
                        }))
                    }
                } else {
                    return Err(PatchErrorKind::CannotAddChildToNonContainer);
                }
                return Ok(None);
            }
            loro::Container::Tree(_) => {
                unimplemented!("Replacing in a tree is not implemented yet");
            }
            loro::Container::MovableList(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                if rest.is_empty() {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let Some(old_val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    l.delete(idx, 1)?;
                    match value {
                        loro::ValueOrContainer::Value(v) => {
                            l.insert(idx, v)?;
                        }
                        loro::ValueOrContainer::Container(c) => {
                            l.insert_container(idx, c)?;
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Replace(ReplaceUndoOperation {
                            path: path.to_buf(),
                            value: old_val,
                        }))
                    }
                } else {
                    let Ok(idx) = idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let Some(mut val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    if let Some(v) = replace_rec(doc, &mut val, rest, value, undo_stack, full_path)?
                    {
                        l.delete(idx, 1)?;
                        match v {
                            loro::ValueOrContainer::Value(v) => {
                                l.insert(idx, v)?;
                            }
                            loro::ValueOrContainer::Container(c) => {
                                l.insert_container(idx, c)?;
                            }
                        }
                    }
                }
                return Ok(None);
            }
            loro::Container::Counter(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::Container::Unknown(_) => {
                return Err(PatchErrorKind::UnknownContainer);
            }
        },
        loro::ValueOrContainer::Value(v) => match v {
            loro::LoroValue::Null => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Bool(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Double(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::I64(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::Binary(_) => {
                return Err(PatchErrorKind::CannotAddChildToNonContainer);
            }
            loro::LoroValue::String(s) => {
                if rest.len() > 0 {
                    return Err(PatchErrorKind::CannotAddChildToNonContainer);
                }
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let Ok(idx) = idx.for_len_incl(s.len()) else {
                    return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                };
                let str_len;
                let old_str;
                let new_value = match value {
                    loro::ValueOrContainer::Value(v) => {
                        if let loro::LoroValue::String(vs) = v {
                            str_len = vs.len();
                            old_str = s
                                .get(idx..idx + str_len)
                                .ok_or(PatchErrorKind::InvalidPointerOOB(first.to_string()))?
                                .to_string();
                            let mut s = (**s).clone();
                            s.replace_range(idx..idx + str_len, &*vs);
                            s
                        } else {
                            return Err(PatchErrorKind::InvalidValue(
                                "expected string value for text container".to_string(),
                            ));
                        }
                    }
                    loro::ValueOrContainer::Container(_) => {
                        return Err(PatchErrorKind::InvalidValue(
                            "expected string value for text container, got container".to_string(),
                        ));
                    }
                };
                if let Some(&mut ref mut undo_stack) = undo_stack {
                    undo_stack.push(UndoOperation::Replace(ReplaceUndoOperation {
                        path: path.to_buf(),
                        value: loro::ValueOrContainer::Value(loro::LoroValue::String(
                            old_str.to_string().into(),
                        )),
                    }))
                }
                Ok(Some(loro::ValueOrContainer::Value(
                    loro::LoroValue::String(new_value.into()),
                )))
            }
            loro::LoroValue::List(l) => {
                let Ok(idx) = first.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(first.to_string()));
                };
                let Ok(idx) = idx.for_len(l.len()) else {
                    return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                };
                let mut new_list = (**l).clone();

                let new_value;
                if rest.is_empty() {
                    let Some(old_val) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    new_value = match value {
                        loro::ValueOrContainer::Value(v) => {
                            new_list.push(v);
                            new_list.swap_remove(idx);
                            loro::ValueOrContainer::Value(loro::LoroValue::List(new_list.into()))
                        }
                        loro::ValueOrContainer::Container(_) => {
                            return Err(PatchErrorKind::CannotAddContainerToNonContainer);
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Replace(ReplaceUndoOperation {
                            path: path.to_buf(),
                            value: loro::ValueOrContainer::Value(old_val.clone()),
                        }))
                    }
                } else {
                    let Some(list_el) = l.get(idx) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(first.to_string()));
                    };
                    let val = replace_rec(
                        doc.clone(),
                        &mut loro::ValueOrContainer::Value(list_el.clone()),
                        rest,
                        value,
                        undo_stack,
                        full_path,
                    )?;
                    if let Some(loro::ValueOrContainer::Value(val)) = val {
                        new_list.push(val);
                        new_list.swap_remove(idx);
                        new_value =
                            loro::ValueOrContainer::Value(loro::LoroValue::List(new_list.into()))
                    } else {
                        unreachable!("replace_rec should return a ValueOrContainer::Value here");
                    }
                }
                Ok(Some(new_value))
            }
            loro::LoroValue::Map(m) => {
                let old = m.get(&first.decoded().to_string());
                let Some(old) = old else {
                    return Err(PatchErrorKind::InvalidPointerNotFound(
                        first.decoded().to_string(),
                    ));
                };
                let mut new_map = (**m).clone();

                let new_value;
                if rest.is_empty() {
                    new_value = match value {
                        loro::ValueOrContainer::Value(v) => {
                            new_map.insert(first.decoded().into(), v);
                            loro::ValueOrContainer::Value(loro::LoroValue::Map(new_map.into()))
                        }
                        loro::ValueOrContainer::Container(_) => {
                            return Err(PatchErrorKind::CannotAddContainerToNonContainer);
                        }
                    };
                    if let Some(&mut ref mut undo_stack) = undo_stack {
                        undo_stack.push(UndoOperation::Add(AddUndoOperation {
                            path: path.to_buf(),
                            value: loro::ValueOrContainer::Value(old.clone()),
                        }))
                    }
                } else {
                    let val = replace_rec(
                        doc.clone(),
                        &mut loro::ValueOrContainer::Value(old.clone()),
                        rest,
                        value,
                        undo_stack,
                        full_path,
                    )?;
                    if let Some(loro::ValueOrContainer::Value(val)) = val {
                        new_map.insert(first.decoded().into(), val);
                        new_value =
                            loro::ValueOrContainer::Value(loro::LoroValue::Map(new_map.into()))
                    } else {
                        if full_path == path {
                            return Ok(None);
                        }
                        unreachable!("replace_rec should return a ValueOrContainer::Value here");
                    }
                }
                return Ok(Some(new_value));
            }
            loro::LoroValue::Container(c) => {
                let container_type = match c {
                    loro::ContainerID::Root {
                        name: _,
                        container_type,
                    } => container_type,
                    loro::ContainerID::Normal {
                        peer: _,
                        counter: _,
                        container_type,
                    } => container_type,
                };
                replace_rec(
                    doc.clone(),
                    &mut loro::ValueOrContainer::Container(match container_type {
                        loro::ContainerType::Text => loro::Container::Text(doc.get_text(c.clone())),
                        loro::ContainerType::Map => loro::Container::Map(doc.get_map(c.clone())),
                        loro::ContainerType::List => loro::Container::List(doc.get_list(c.clone())),
                        loro::ContainerType::MovableList => {
                            loro::Container::MovableList(doc.get_movable_list(c.clone()))
                        }
                        loro::ContainerType::Tree => loro::Container::Tree(doc.get_tree(c.clone())),
                        loro::ContainerType::Counter => {
                            loro::Container::Counter(doc.get_counter(c.clone()))
                        }
                        loro::ContainerType::Unknown(_) => {
                            return Err(PatchErrorKind::UnknownContainer);
                        }
                    }),
                    path,
                    value,
                    undo_stack,
                    full_path,
                )
            }
        },
    }
}

fn replace(
    doc: std::sync::Arc<loro::LoroDoc>,
    path: &jsonptr::Pointer,
    value: serde_json::Value,
    undo_stack: Option<&mut Vec<UndoOperation>>,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    let loro_value =
        super::serde::serde_to_loro(value).map_err(PatchErrorKind::CannotConvertJsonToLoro)?;
    let parent = doc.get_value();

    match replace_rec(
        doc,
        &mut loro::ValueOrContainer::Value(parent),
        path,
        loro_value,
        undo_stack,
        &path.to_buf(),
    ) {
        Err(e) => Err(e),
        Ok(_) => Ok(()),
    }
}

fn replace_loro(
    doc: std::sync::Arc<loro::LoroDoc>,
    path: &jsonptr::Pointer,
    value: loro::ValueOrContainer,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    let parent = doc.get_value();

    match replace_rec(
        doc,
        &mut loro::ValueOrContainer::Value(parent),
        path,
        value,
        None,
        &path.to_buf(),
    ) {
        Err(e) => Err(e),
        Ok(_) => Ok(()),
    }
}

fn mov(
    doc: std::sync::Arc<loro::LoroDoc>,
    from: &jsonptr::Pointer,
    path: &jsonptr::Pointer,
    allow_last: bool,
    undo_stack: Option<&mut Vec<UndoOperation>>,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() || from.is_empty() {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    if path.starts_with(from) && path.len() != from.len() {
        return Err(PatchErrorKind::CannotMoveInsideItself);
    }

    let Some((from_first, from_last)) = from.split_back() else {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };
    let Some((path_first, path_last)) = path.split_back() else {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    if from_first == path_first {
        let parent = safe_get_by_str_path(doc.clone(), &from_first);
        if let Some(loro::ValueOrContainer::Container(c)) = parent {
            if let loro::Container::MovableList(l) = c {
                let Ok(from_idx) = from_last.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(
                        from_last.to_string(),
                    ));
                };
                let from_idx = if allow_last && from_idx == jsonptr::index::Index::Next {
                    if l.len() == 0 {
                        return Err(PatchErrorKind::InvalidPointerOOB(from_last.to_string()));
                    }
                    l.len() - 1
                } else {
                    let Ok(idx) = from_idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(from_last.to_string()));
                    };
                    idx
                };
                let Ok(path_idx) = path_last.to_index() else {
                    return Err(PatchErrorKind::InvalidPointerNonIndex(
                        path_last.to_string(),
                    ));
                };
                let path_idx = if allow_last && path_idx == jsonptr::index::Index::Next {
                    if l.len() == 0 {
                        return Err(PatchErrorKind::InvalidPointerOOB(path_last.to_string()));
                    }
                    l.len() - 1
                } else {
                    let Ok(idx) = path_idx.for_len(l.len()) else {
                        return Err(PatchErrorKind::InvalidPointerOOB(path_last.to_string()));
                    };
                    idx
                };
                if from_idx == path_idx {
                    return Ok(()); // No need to move if the indices are the same.
                }
                l.mov(from_idx, path_idx)?;
                if let Some(&mut ref mut undo_stack) = undo_stack {
                    undo_stack.push(UndoOperation::Move(MoveUndoOperation {
                        path: from.to_buf(),
                        from: path.to_buf(),
                    }))
                }
                return Ok(());
            } else if let loro::Container::Tree(_) = c {
                // TODO: Implement moving a tree.
            }
        }
    }
    let new_undo_stack = match undo_stack {
        Some(stack) => stack,
        None => &mut Vec::new(),
    };
    remove(doc.clone(), from, allow_last, Some(new_undo_stack))?;
    let UndoOperation::Add(undo_op) = &new_undo_stack[new_undo_stack.len() - 1] else {
        unreachable!("Last operation in undo stack should be an Add operation");
    };
    add_loro(
        doc.clone(),
        path,
        undo_op.value.clone(),
        Some(new_undo_stack),
    )?;

    Ok(())
}

fn copy(
    doc: std::sync::Arc<loro::LoroDoc>,
    from: &jsonptr::Pointer,
    path: &jsonptr::Pointer,
    undo_stack: Option<&mut Vec<UndoOperation>>,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() || from.is_empty() {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    let from_val = safe_get_by_str_path(doc.clone(), &from);
    if let Some(loro::ValueOrContainer::Value(v)) = from_val {
        add_loro(
            doc,
            path,
            loro::ValueOrContainer::Value(v.clone()),
            undo_stack,
        )?;
    } else {
        return Err(PatchErrorKind::CopyMustOnlyCopyValue);
    }
    Ok(())
}

fn test(
    doc: std::sync::Arc<loro::LoroDoc>,
    path: &jsonptr::Pointer,
    expected: &serde_json::Value,
) -> Result<(), PatchErrorKind> {
    if path.is_empty() {
        return Err(PatchErrorKind::CannotAddToRootWithoutCrdtValue);
    };

    let val = safe_get_by_str_path(doc.clone(), &path)
        .ok_or(PatchErrorKind::InvalidPointerNotFound(path.to_string()))?;
    let loro::ValueOrContainer::Value(expected_loro) =
        super::serde::serde_to_loro(expected.clone())
            .map_err(PatchErrorKind::CannotConvertJsonToLoro)?
    else {
        return Err(PatchErrorKind::TestMustOnlyTestValue);
    };
    if val.get_deep_value() != expected_loro {
        return Err(PatchErrorKind::TestFailed);
    }
    Ok(())
}

fn apply_patches(
    doc: std::sync::Arc<loro::LoroDoc>,
    patches: Patch,
    undo_stack: Option<&mut Vec<UndoOperation>>,
) -> Result<(), PatchError> {
    for (operation, patch) in patches.iter().enumerate() {
        let doc = doc.clone();
        match patch {
            PatchOperation::Add(op) => match undo_stack {
                Some(&mut ref mut undo_stack) => {
                    add(doc, &op.path, op.value.clone(), Some(undo_stack))
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
                None => {
                    add(doc, &op.path, op.value.clone(), None)
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
            },
            PatchOperation::Remove(op) => match undo_stack {
                Some(&mut ref mut undo_stack) => {
                    remove(doc, &op.path, false, Some(undo_stack))
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
                None => {
                    remove(doc, &op.path, false, None)
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
            },
            PatchOperation::Replace(op) => match undo_stack {
                Some(&mut ref mut undo_stack) => {
                    replace(doc, &op.path, op.value.clone(), Some(undo_stack))
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
                None => {
                    replace(doc, &op.path, op.value.clone(), None)
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
            },
            PatchOperation::Move(op) => match undo_stack {
                Some(&mut ref mut undo_stack) => {
                    mov(doc, &op.from, &op.path, false, Some(undo_stack))
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
                None => {
                    mov(doc, &op.from, &op.path, false, None)
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
            },
            PatchOperation::Copy(op) => match undo_stack {
                Some(&mut ref mut undo_stack) => {
                    copy(doc, &op.from, &op.path, Some(undo_stack))
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
                None => {
                    copy(doc, &op.from, &op.path, None)
                        .map_err(|e| translate_error(e, operation, &op.path))?;
                }
            },
            PatchOperation::Test(op) => {
                test(doc, &op.path, &op.value)
                    .map_err(|e| translate_error(e, operation, &op.path))?;
            }
        };
    }

    Ok(())
}

fn undo_patches(
    doc: std::sync::Arc<loro::LoroDoc>,
    undo_patches: &[UndoOperation],
) -> Result<(), PatchError> {
    for (operation, patch) in undo_patches.iter().enumerate().rev() {
        let doc = doc.clone();
        match patch {
            UndoOperation::Add(op) => {
                add_loro(doc, &op.path, op.value.clone(), None)
                    .map_err(|e| translate_error(e, operation, &op.path))?;
            }
            UndoOperation::Remove(op) => {
                remove(doc, &op.path, true, None)
                    .map_err(|e| translate_error(e, operation, &op.path))?;
            }
            UndoOperation::Replace(op) => {
                replace_loro(doc, &op.path, op.value.clone())
                    .map_err(|e| translate_error(e, operation, &op.path))?;
            }
            UndoOperation::Move(op) => {
                mov(doc, &op.from, &op.path, true, None)
                    .map_err(|e| translate_error(e, operation, &op.path))?;
            }
        }
    }

    Ok(())
}

pub fn patch_loro_document(
    doc: std::sync::Arc<loro::LoroDoc>,
    patch: Patch,
) -> Result<(), PatchLoroDocumentError> {
    // TODO: think about either forking or locking to prevent concurrent modifications
    let mut undo_stack = Vec::<UndoOperation>::with_capacity(patch.len());
    if let Err(e) = apply_patches(doc.clone(), patch, Some(&mut undo_stack)) {
        if let Err(e) = undo_patches(doc, &undo_stack) {
            return Err(PatchLoroDocumentError::UndoPatchesError(e));
        }
        return Err(PatchLoroDocumentError::ApplyPatchesError(e));
    }

    Ok(())
}
