use thiserror::Error;

#[derive(Error, Debug)]
pub enum InvalidTimestampError {
    #[error("Invalid timestamp format: {0}")]
    InvalidTimestampFormat(String),
}

pub fn from_timestamp(timestamp: chrono::DateTime<chrono::Utc>) -> String {
    let seconds = timestamp.timestamp();
    let nanoseconds = timestamp.timestamp_subsec_nanos();
    let formatted_timestamp = format!("{}.{}", seconds, nanoseconds);
    formatted_timestamp
}

pub fn to_timestamp(
    timestamp: &str,
) -> Result<chrono::DateTime<chrono::Utc>, InvalidTimestampError> {
    let mut split_timestamp = timestamp.split(".");
    let seconds = split_timestamp
        .next()
        .ok_or(InvalidTimestampError::InvalidTimestampFormat(
            "Missing seconds".to_string(),
        ))?
        .parse::<i64>()
        .map_err(|e| InvalidTimestampError::InvalidTimestampFormat(e.to_string()))?;
    let nanoseconds = split_timestamp
        .next()
        .ok_or(InvalidTimestampError::InvalidTimestampFormat(
            "Missing nanoseconds".to_string(),
        ))?
        .parse::<u32>()
        .map_err(|e| InvalidTimestampError::InvalidTimestampFormat(e.to_string()))?;

    let timestamp = chrono::DateTime::from_timestamp(seconds, nanoseconds).ok_or(
        InvalidTimestampError::InvalidTimestampFormat("Timestamp out of range".to_string()),
    )?;
    Ok(timestamp)
}

pub fn from_now() -> String {
    from_timestamp(chrono::Utc::now())
}
