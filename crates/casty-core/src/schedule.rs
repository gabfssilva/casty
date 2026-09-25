//! What a key scheduled for itself: a message it tells itself at a time of the wall clock, once or on an interval,
//! under a name of its choosing, which no two of its schedules share.
//!
//! The schedules of a key are part of its state, in the reserved page `@schedules`, so whichever node runs the key
//! next takes them up where the last one left them. A time is in microseconds since the Unix epoch, and a period in
//! microseconds.

use crate::schema::msgpack::Malformed;
use crate::wire::{Reading, Writer};

/// Reserved page with the schedules of a key. A key that has none has no such page.
pub const SCHEDULES: &str = "@schedules";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    pub name: String,
    /// The message, written with the message schema of the type of the key.
    pub message: Vec<u8>,
    /// When it goes off next.
    pub due: i64,
    /// How long after each time it goes off it goes off again. Without it, it goes off once.
    pub every: Option<u64>,
}

impl Schedule {
    /// When it goes off again after going off at `due`, now being `now`: the first time of its interval later than
    /// `now`. A loop that fired it late, or a node that took the key over late, skips the times it missed instead of
    /// sending them all at once. Nothing for a schedule that goes off once.
    #[must_use]
    pub fn after(&self, now: i64) -> Option<i64> {
        let every = i64::try_from(self.every?).ok().filter(|every| *every > 0)?;
        let missed = now.saturating_sub(self.due).max(0) / every;
        Some(
            self.due
                .saturating_add(every.saturating_mul(missed.saturating_add(1))),
        )
    }
}

/// The page of `schedules`: a `Schedules` holding the list of them, so that a later version can add fields this one
/// steps over.
#[must_use]
pub fn encode(schedules: &[Schedule]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Schedules", 1);
    writer.name("schedules");
    writer.items(schedules.len());
    for schedule in schedules {
        writer.fields(4);
        writer.name("name");
        writer.text(&schedule.name);
        writer.name("message");
        writer.bytes(&schedule.message);
        writer.name("due");
        writer.int(schedule.due);
        writer.name("every");
        match schedule.every {
            Some(every) => writer.unsigned(every),
            None => writer.nil(),
        }
    }
    writer.finish()
}

pub fn decode(page: &[u8]) -> Result<Vec<Schedule>, Malformed> {
    let mut reading = Reading::new(page);
    let (tag, fields) = reading.tagged()?;
    if tag != "Schedules" {
        return Err(Malformed::Marker(0));
    }
    let mut schedules = Vec::new();
    for _ in 0..fields {
        if reading.name()? != "schedules" {
            reading.skip()?;
            continue;
        }
        for _ in 0..reading.items()? {
            schedules.push(one(&mut reading)?);
        }
    }
    Ok(schedules)
}

fn one(reading: &mut Reading<'_>) -> Result<Schedule, Malformed> {
    let (mut name, mut message, mut due, mut every) = (None, None, None, None);
    for _ in 0..reading.fields()? {
        match reading.name()? {
            "name" => name = Some(reading.text()?),
            "message" => message = Some(reading.bytes()?),
            "due" => due = Some(reading.int()?),
            "every" => {
                every = if reading.nil()? {
                    None
                } else {
                    Some(reading.unsigned()?)
                };
            }
            _ => reading.skip()?,
        }
    }
    let (Some(name), Some(message), Some(due)) = (name, message, due) else {
        return Err(Malformed::Truncated);
    };
    Ok(Schedule {
        name,
        message,
        due,
        every,
    })
}

#[cfg(test)]
mod tests {
    use super::{Schedule, decode, encode};
    use crate::wire::Writer;

    fn every(due: i64, every: Option<u64>) -> Schedule {
        Schedule {
            name: "poll".to_owned(),
            message: b"tick".to_vec(),
            due,
            every,
        }
    }

    #[test]
    fn a_schedule_that_goes_off_once_does_not_go_off_again() {
        assert_eq!(every(100, None).after(100), None);
    }

    #[test]
    fn a_schedule_on_time_goes_off_again_one_interval_later() {
        assert_eq!(every(100, Some(10)).after(100), Some(110));
        assert_eq!(every(100, Some(10)).after(104), Some(110));
    }

    #[test]
    fn a_schedule_fired_late_skips_the_times_it_missed() {
        assert_eq!(every(100, Some(10)).after(125), Some(130));
        assert_eq!(every(100, Some(10)).after(130), Some(140));
    }

    #[test]
    fn a_schedule_fired_early_does_not_go_off_before_its_interval() {
        assert_eq!(every(100, Some(10)).after(90), Some(110));
    }

    #[test]
    fn schedules_read_back_as_they_were_written() {
        let schedules = vec![every(100, Some(10)), every(-5, None)];
        assert_eq!(decode(&encode(&schedules)), Ok(schedules));
        assert_eq!(decode(&encode(&[])), Ok(Vec::new()));
    }

    #[test]
    fn a_page_that_is_not_one_of_schedules_is_malformed() {
        let mut other = Writer::new();
        other.tagged("State", 0);
        assert!(decode(&other.finish()).is_err());
        assert!(decode(b"\x90").is_err());
    }

    #[test]
    fn a_field_a_later_version_added_is_stepped_over() {
        let mut later = Writer::new();
        later.tagged("Schedules", 2);
        later.name("schedules");
        later.items(1);
        later.fields(5);
        later.name("name");
        later.text("poll");
        later.name("paused");
        later.bool(true);
        later.name("message");
        later.bytes(b"tick");
        later.name("due");
        later.int(100);
        later.name("every");
        later.nil();
        later.name("owner");
        later.text("someone");
        assert_eq!(decode(&later.finish()), Ok(vec![every(100, None)]));
    }
}
