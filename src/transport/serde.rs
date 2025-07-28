use base64::prelude::*;
use loro::ContainerTrait;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct CRDTContainer {
    #[serde(rename = "🦜")]
    typ: u8,
    #[serde(rename = "v")]
    content: CRDTValue,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum CRDTValue {
    Container(std::boxed::Box<CRDTContainer>),
    Value(serde_json::Value),
    Array(Vec<CRDTValue>),
    Map(std::collections::BTreeMap<String, CRDTValue>),
}

#[derive(thiserror::Error, Debug)]
pub enum LoroToSerdeError {
    #[error("invalid container type")]
    InvalidContainerType(),
}

pub fn loro_to_serde(val: loro::ValueOrContainer) -> Result<CRDTValue, LoroToSerdeError> {
    Ok(match val {
        loro::ValueOrContainer::Value(v) => match v {
            loro::LoroValue::Null => CRDTValue::Value(serde_json::Value::Null),
            loro::LoroValue::Bool(b) => CRDTValue::Value(serde_json::Value::Bool(b)),
            loro::LoroValue::Double(f) => CRDTValue::Value(
                serde_json::Number::from_f64(f)
                    .map_or(serde_json::Value::Null, serde_json::Value::Number),
            ),
            loro::LoroValue::I64(i) => CRDTValue::Value(
                serde_json::Number::from_i128(i128::from(i))
                    .map_or(serde_json::Value::Null, serde_json::Value::Number),
            ),
            loro::LoroValue::Binary(b) => CRDTValue::Value(serde_json::Value::String(
                BASE64_STANDARD.encode((*b).clone()),
            )),
            loro::LoroValue::String(s) => CRDTValue::Value(serde_json::Value::String((*s).clone())),
            loro::LoroValue::List(l) => {
                let values: Result<Vec<_>, _> = l
                    .iter()
                    .map(|x| loro_to_serde(loro::ValueOrContainer::Value((*x).clone())))
                    .collect();
                CRDTValue::Array(values?)
            }
            loro::LoroValue::Map(m) => {
                let map: Result<std::collections::BTreeMap<_, _>, _> = m
                    .iter()
                    .map(|(k, v)| {
                        loro_to_serde(loro::ValueOrContainer::Value((*v).clone()))
                            .map(|v| (k.to_string(), v))
                    })
                    .collect();
                CRDTValue::Map(map?)
            }
            loro::LoroValue::Container(c) => {
                CRDTValue::Value(serde_json::Value::String(c.to_loro_value_string()))
            }
        },
        loro::ValueOrContainer::Container(c) => {
            CRDTValue::Container(std::boxed::Box::new(CRDTContainer {
                typ: c.get_type().to_u8(),
                content: match c {
                    loro::Container::List(l) => {
                        loro_to_serde(loro::ValueOrContainer::Value(l.get_value()))?
                    }
                    loro::Container::Map(m) => {
                        loro_to_serde(loro::ValueOrContainer::Value(m.get_value()))?
                    }
                    loro::Container::Text(t) => {
                        loro_to_serde(loro::ValueOrContainer::Value(t.get_richtext_value()))?
                    }
                    loro::Container::Tree(t) => {
                        loro_to_serde(loro::ValueOrContainer::Value(t.get_value()))?
                    }
                    loro::Container::MovableList(ml) => {
                        loro_to_serde(loro::ValueOrContainer::Value(ml.get_value()))?
                    }
                    loro::Container::Counter(c) => CRDTValue::Value(
                        serde_json::Number::from_f64(c.get_value())
                            .map_or(serde_json::Value::Null, serde_json::Value::Number),
                    ),
                    loro::Container::Unknown(_) => {
                        return Err(LoroToSerdeError::InvalidContainerType());
                    }
                },
            }))
        }
    })
}

#[derive(thiserror::Error, Debug)]
pub enum SerdeToLoroError {
    #[error("invalid number type")]
    InvalidNumberType(),
    #[error("invalid container type: {0}")]
    InvalidContainerType(serde_json::Value),
    #[error("container has no value")]
    ContainerHasNoValue(),
    #[error("invalid container value: {0}")]
    InvalidContainerValue(serde_json::Value),
    #[error("error while writing loro value: {0}")]
    WriteError(loro::LoroError),
    #[error("cannot assign a non-map value to loro doc root")]
    CannotAssignNonMapToRoot(),
    #[error("cannot assign a non-crdt value to loro doc root")]
    CannotAssignNonCRDTValueToRoot(),
}

pub fn serde_to_loro(val: serde_json::Value) -> Result<loro::ValueOrContainer, SerdeToLoroError> {
    serde_to_loro_int(val, None, None)
}

pub fn serde_to_loro_root(
    val: serde_json::Value,
    root: Option<std::sync::Arc<loro::LoroDoc>>,
) -> Result<loro::ValueOrContainer, SerdeToLoroError> {
    serde_to_loro_int(val, root, None)
}

fn serde_to_loro_int(
    val: serde_json::Value,
    root: Option<std::sync::Arc<loro::LoroDoc>>,
    root_container: Option<&str>,
) -> Result<loro::ValueOrContainer, SerdeToLoroError> {
    if root.is_some() && root_container.is_none() && !val.is_object() {
        return Err(SerdeToLoroError::CannotAssignNonMapToRoot());
    } else if root_container.is_some() {
        if !val.is_object() || val.get("🦜").is_none() {
            return Err(SerdeToLoroError::CannotAssignNonCRDTValueToRoot());
        }
    }

    match val {
        serde_json::Value::Null => Ok(loro::ValueOrContainer::Value(loro::LoroValue::Null)),
        serde_json::Value::Bool(b) => Ok(loro::ValueOrContainer::Value(loro::LoroValue::Bool(b))),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(loro::ValueOrContainer::Value(loro::LoroValue::I64(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(loro::ValueOrContainer::Value(loro::LoroValue::Double(f)))
            } else {
                Err(SerdeToLoroError::InvalidNumberType())
            }
        }
        serde_json::Value::String(s) => {
            if let Some(v) = loro::ContainerID::try_from_loro_value_string(&s) {
                Ok(loro::ValueOrContainer::Value(loro::LoroValue::Container(v)))
            } else {
                Ok(loro::ValueOrContainer::Value(loro::LoroValue::String(
                    loro::LoroStringValue::from(s),
                )))
            }
        }
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<_>, _> = arr.into_iter().map(serde_to_loro).collect();
            let values = values?;
            let values: Vec<loro::LoroValue> = values
                .iter()
                .map(|x| match x {
                    loro::ValueOrContainer::Value(v) => (*v).clone(),
                    loro::ValueOrContainer::Container(c) => loro::LoroValue::Container(c.id()),
                })
                .collect();
            Ok(loro::ValueOrContainer::Value(loro::LoroValue::List(
                loro::LoroListValue::from(values),
            )))
        }
        serde_json::Value::Object(obj) => {
            if let Some(v) = obj.get("🦜") {
                let vnum = v.as_u64();
                let Some(vnum) = vnum else {
                    return Err(SerdeToLoroError::InvalidContainerType(v.clone()));
                };
                let vnum = u8::try_from(vnum)
                    .map_err(|_| SerdeToLoroError::InvalidContainerType(v.clone()))?;
                let container_type = loro::ContainerType::try_from_u8(vnum);
                let container_value = obj
                    .get("v")
                    .ok_or(SerdeToLoroError::ContainerHasNoValue())?;
                match container_type {
                    Ok(c) => match c {
                        loro::ContainerType::Text => {
                            // TODO: Handle rich text content
                            if container_value.is_array() {
                                unimplemented!("Rich text content is not supported yet");
                            }

                            let serde_json::Value::String(s) = container_value else {
                                return Err(SerdeToLoroError::InvalidContainerValue(
                                    container_value.clone(),
                                ));
                            };
                            let container = if let (Some(r), Some(id)) = (root, root_container) {
                                r.get_text(id)
                            } else {
                                loro::LoroText::new()
                            };
                            container
                                .insert(0, s)
                                .map_err(SerdeToLoroError::WriteError)?;
                            return Ok(loro::ValueOrContainer::Container(loro::Container::Text(
                                container,
                            )));
                        }
                        loro::ContainerType::Map => {
                            let serde_json::Value::Object(map) = container_value else {
                                return Err(SerdeToLoroError::InvalidContainerValue(
                                    container_value.clone(),
                                ));
                            };
                            let container = if let (Some(r), Some(id)) = (root, root_container) {
                                r.get_map(id)
                            } else {
                                loro::LoroMap::new()
                            };
                            for (k, v) in map {
                                let v = serde_to_loro(v.clone())?;
                                match v {
                                    loro::ValueOrContainer::Value(v) => {
                                        container
                                            .insert(k, v)
                                            .map_err(SerdeToLoroError::WriteError)?;
                                    }
                                    loro::ValueOrContainer::Container(c) => {
                                        container
                                            .insert(k, loro::LoroValue::Container(c.id()))
                                            .map_err(SerdeToLoroError::WriteError)?;
                                    }
                                }
                            }
                            return Ok(loro::ValueOrContainer::Container(loro::Container::Map(
                                container,
                            )));
                        }
                        loro::ContainerType::List => {
                            let serde_json::Value::Array(arr) = container_value else {
                                return Err(SerdeToLoroError::InvalidContainerValue(
                                    container_value.clone(),
                                ));
                            };
                            let values: Result<Vec<_>, _> =
                                arr.into_iter().map(|x| serde_to_loro(x.clone())).collect();
                            let values = values?;
                            let values: Vec<loro::LoroValue> = values
                                .iter()
                                .map(|x| match x {
                                    loro::ValueOrContainer::Value(v) => (*v).clone(),
                                    loro::ValueOrContainer::Container(c) => {
                                        loro::LoroValue::Container(c.id())
                                    }
                                })
                                .collect();
                            let container = if let (Some(r), Some(id)) = (root, root_container) {
                                r.get_list(id)
                            } else {
                                loro::LoroList::new()
                            };
                            for value in values {
                                container
                                    .push(value)
                                    .map_err(SerdeToLoroError::WriteError)?;
                            }
                            return Ok(loro::ValueOrContainer::Container(loro::Container::List(
                                container,
                            )));
                        }
                        loro::ContainerType::MovableList => {
                            let serde_json::Value::Array(arr) = container_value else {
                                return Err(SerdeToLoroError::InvalidContainerValue(
                                    container_value.clone(),
                                ));
                            };
                            let values: Result<Vec<_>, _> =
                                arr.into_iter().map(|x| serde_to_loro(x.clone())).collect();
                            let values = values?;
                            let values: Vec<loro::LoroValue> = values
                                .iter()
                                .map(|x| match x {
                                    loro::ValueOrContainer::Value(v) => (*v).clone(),
                                    loro::ValueOrContainer::Container(c) => {
                                        loro::LoroValue::Container(c.id())
                                    }
                                })
                                .collect();
                            let container = if let (Some(r), Some(id)) = (root, root_container) {
                                r.get_movable_list(id)
                            } else {
                                loro::LoroMovableList::new()
                            };
                            for value in values {
                                container
                                    .push(value)
                                    .map_err(SerdeToLoroError::WriteError)?;
                            }
                            return Ok(loro::ValueOrContainer::Container(
                                loro::Container::MovableList(container),
                            ));
                        }
                        loro::ContainerType::Tree => {
                            if !container_value.is_null() {
                                unimplemented!("Tree content is not supported yet");
                            }
                            let container = if let (Some(r), Some(id)) = (root, root_container) {
                                r.get_tree(id)
                            } else {
                                loro::LoroTree::new()
                            };
                            return Ok(loro::ValueOrContainer::Container(loro::Container::Tree(
                                container,
                            )));
                        }
                        loro::ContainerType::Counter => {
                            let serde_json::Value::Number(n) = container_value else {
                                return Err(SerdeToLoroError::InvalidContainerValue(
                                    container_value.clone(),
                                ));
                            };
                            let f = n.as_f64().ok_or(SerdeToLoroError::InvalidContainerValue(
                                container_value.clone(),
                            ))?;
                            let container = if let (Some(r), Some(id)) = (root, root_container) {
                                r.get_counter(id)
                            } else {
                                loro::LoroCounter::new()
                            };
                            container
                                .increment(f)
                                .map_err(SerdeToLoroError::WriteError)?;
                            return Ok(loro::ValueOrContainer::Container(
                                loro::Container::Counter(container),
                            ));
                        }
                        loro::ContainerType::Unknown(_) => {
                            return Err(SerdeToLoroError::InvalidContainerType(v.clone()));
                        }
                    },
                    Err(_) => {
                        return Err(SerdeToLoroError::InvalidContainerType(v.clone()));
                    }
                };
            }

            let map: Result<std::collections::BTreeMap<_, _>, _> = obj
                .into_iter()
                .map(|(k, v)| {
                    (if root.is_some() && root_container.is_none() {
                        serde_to_loro_int(v, root.clone(), Some(&k))
                    } else {
                        serde_to_loro(v)
                    })
                    .map(|v| (k, v))
                })
                .collect();
            let map = map?;
            let map: std::collections::HashMap<_, _> = map
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        match v {
                            loro::ValueOrContainer::Value(v) => v,
                            loro::ValueOrContainer::Container(c) => {
                                loro::LoroValue::Container(c.id())
                            }
                        },
                    )
                })
                .collect();
            Ok(loro::ValueOrContainer::Value(loro::LoroValue::Map(
                loro::LoroMapValue::from(map),
            )))
        }
    }
}
