//! How an annotation is written in a message, which the tests of the refusal read word for word.

use pyo3::prelude::*;
use pyo3::types::PyType;

use super::introspect::Introspect;

/// The annotation as it is written in source, which is what a `SchemaError` names.
pub fn name(introspect: &Introspect<'_>, annotation: &Bound<'_, PyAny>) -> PyResult<String> {
    let origin = introspect.origin(annotation)?;
    let args = introspect.args(annotation)?;
    if origin.is(&introspect.union_type) || origin.is(&introspect.union) {
        let parts: PyResult<Vec<String>> = args.iter().map(|arg| name(introspect, arg)).collect();
        return Ok(parts?.join(" | "));
    }
    if !origin.is_none() && !origin.is(&introspect.literal) {
        let parts: PyResult<Vec<String>> = args.iter().map(|arg| name(introspect, arg)).collect();
        return Ok(format!(
            "{}[{}]",
            name(introspect, &origin)?,
            parts?.join(", ")
        ));
    }
    if annotation.is(&introspect.ellipsis) {
        return Ok("...".to_owned());
    }
    if annotation.is_none() || annotation.is(&introspect.none_type) {
        return Ok("None".to_owned());
    }
    if let Ok(class) = annotation.cast::<PyType>() {
        return class.getattr("__qualname__")?.extract::<String>();
    }
    if annotation.is_instance(&introspect.type_alias_type)?
        || annotation.is_instance(&introspect.type_var)?
    {
        return annotation.getattr("__name__")?.extract::<String>();
    }
    Ok(annotation.repr()?.to_str()?.to_owned())
}

/// The container the annotation should have used instead, for the three mutable ones that are refused.
pub fn replacement(
    introspect: &Introspect<'_>,
    container: &Bound<'_, PyAny>,
    args: &[Bound<'_, PyAny>],
) -> PyResult<Option<String>> {
    let (template, placeholder) = if container.is(&introspect.list_type) {
        ("tuple[{}, ...]", "T")
    } else if container.is(&introspect.set_type) {
        ("frozenset[{}]", "T")
    } else if container.is(&introspect.dict_type) {
        ("Mapping[{}]", "K, V")
    } else {
        return Ok(None);
    };
    let parts: PyResult<Vec<String>> = args.iter().map(|arg| name(introspect, arg)).collect();
    let inside = parts?.join(", ");
    let inside = if inside.is_empty() {
        placeholder.to_owned()
    } else {
        inside
    };
    Ok(Some(template.replace("{}", &inside)))
}
