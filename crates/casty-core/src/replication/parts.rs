//! How the state of a key is cut to travel in messages of a bounded size, and put back together.
//!
//! A part carries the next bytes of each page it names. Whole pages share a part while they fit, and a page larger
//! than the room left goes on in the parts that follow, so the pages are the parts appended in order. Whoever puts
//! them back together appends them in the order they are numbered, and exposes nothing before the last one is in.

use std::collections::btree_map::Entry;

use super::messages::Copy;
use crate::store::Pages;

/// Bytes a page of `size` bytes takes out of the message limit: its name, its data and the headers around both.
#[must_use]
pub fn cost(name: &str, size: usize) -> usize {
    name.len() + size + PAGE
}

/// The size of each page, in the order of their names.
#[must_use]
pub fn sizes(pages: &Pages) -> Vec<(String, usize)> {
    pages
        .iter()
        .map(|(name, data)| (name.clone(), data.len()))
        .collect()
}

/// The bytes `start..end` of the page `name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Piece {
    pub name: String,
    pub start: usize,
    pub end: usize,
}

/// Where pages of these sizes are cut so that each part fits in `limit` bytes. There is always a part, empty when
/// there are no pages.
///
/// A page enters a part with one byte of it at least, which a long enough name leaves no room for. Such a page goes
/// whole and alone, over the limit: the owner refuses it before anything is cut.
#[must_use]
pub fn plan(sizes: &[(String, usize)], limit: usize) -> Vec<Vec<Piece>> {
    let mut parts: Vec<Vec<Piece>> = vec![Vec::new()];
    let mut held = 0;
    for (name, size) in sizes {
        let mut start = 0;
        loop {
            let rest = *size - start;
            let fits = held + cost(name, usize::from(rest > 0)) <= limit;
            let part = parts.last_mut().expect("a part is always open");
            if !fits && !part.is_empty() {
                parts.push(Vec::new());
                held = 0;
                continue;
            }
            let end = if fits {
                start + rest.min(limit - held - cost(name, 0))
            } else {
                *size
            };
            part.push(Piece {
                name: name.clone(),
                start,
                end,
            });
            held += cost(name, end - start);
            start = end;
            if start == *size {
                break;
            }
        }
    }
    parts
}

/// The bytes a part names, out of the pages it was planned on.
#[must_use]
pub fn cut(pages: &Pages, part: &[Piece]) -> Pages {
    part.iter()
        .filter_map(|piece| {
            let data = pages.get(&piece.name)?.get(piece.start..piece.end)?;
            Some((piece.name.clone(), data.to_vec()))
        })
        .collect()
}

/// The pages cut in parts that each fit in `limit` bytes.
#[must_use]
pub fn split(pages: &Pages, limit: usize) -> Vec<Pages> {
    plan(&sizes(pages), limit)
        .iter()
        .map(|part| cut(pages, part))
        .collect()
}

/// Add the bytes of a part to the pages they go on from.
pub fn append(pages: &mut Pages, part: Pages) {
    for (name, data) in part {
        match pages.entry(name) {
            Entry::Vacant(vacant) => {
                vacant.insert(data);
            }
            Entry::Occupied(mut held) => held.get_mut().extend_from_slice(&data),
        }
    }
}

/// Copies grouped in order so that each group fits in `limit` bytes. There is always a group, empty when there are
/// no copies.
///
/// A copy counts what it carries besides its pages as well: a range of small keys puts thousands of them in one
/// message, and what surrounds each one adds up past the room a message keeps around its content.
#[must_use]
pub fn packed(copies: Vec<Copy>, limit: usize) -> Vec<Vec<Copy>> {
    let mut groups: Vec<Vec<Copy>> = vec![Vec::new()];
    let mut held = 0;
    for copy in copies {
        let pages: usize = copy
            .pages
            .iter()
            .map(|(name, data)| cost(name, data.len()))
            .sum();
        let weight = COPY + cost(&copy.key, pages);
        let last = groups.last_mut().expect("a group is always open");
        if !last.is_empty() && held + weight > limit {
            groups.push(vec![copy]);
            held = weight;
        } else {
            last.push(copy);
            held += weight;
        }
    }
    groups
}

/// The headers msgpack writes around the name and the data of a page.
const PAGE: usize = 16;

/// Bytes a copy carries besides its key and its pages: the write and the promise it holds, the address of the node
/// that names each, and the names of its fields.
const COPY: usize = 1024;

#[cfg(test)]
mod tests {
    use super::super::messages::Copy;
    use super::{COPY, Piece, append, cost, packed, plan, split};
    use crate::store::Pages;

    fn pages(entries: &[(&str, &[u8])]) -> Pages {
        entries
            .iter()
            .map(|(name, data)| ((*name).to_owned(), (*data).to_vec()))
            .collect()
    }

    fn piece(name: &str, start: usize, end: usize) -> Piece {
        Piece {
            name: name.to_owned(),
            start,
            end,
        }
    }

    /// What a part takes out of the message limit.
    fn weight(part: &Pages) -> usize {
        part.iter().map(|(name, data)| cost(name, data.len())).sum()
    }

    #[test]
    fn a_page_larger_than_a_message_is_cut_in_parts_that_each_fit_and_add_up_to_it() {
        let large: Vec<u8> = (0..=u8::MAX).cycle().take(1_000).collect();
        let written = pages(&[
            ("@active", b""),
            ("entries", large.as_slice()),
            ("owner", b"ana"),
        ]);

        let parts = split(&written, 64);

        assert!(
            parts.len() > 1_000 / 64,
            "{} parts for a page of 1000 bytes",
            parts.len()
        );
        for part in &parts {
            assert!(weight(part) <= 64, "a part takes {} bytes", weight(part));
        }
        let mut rebuilt = Pages::new();
        for part in parts {
            append(&mut rebuilt, part);
        }
        assert_eq!(rebuilt, written);
    }

    #[test]
    fn pages_share_a_part_while_they_fit_and_a_large_one_goes_on_in_the_next() {
        let sizes = vec![
            ("a".to_owned(), 10),
            ("b".to_owned(), 10),
            ("c".to_owned(), 100),
            ("d".to_owned(), 1),
        ];

        assert_eq!(
            plan(&sizes, 64),
            vec![
                vec![piece("a", 0, 10), piece("b", 0, 10)],
                vec![piece("c", 0, 47)],
                vec![piece("c", 47, 94)],
                vec![piece("c", 94, 100), piece("d", 0, 1)],
            ]
        );
    }

    #[test]
    fn nothing_to_cut_is_one_empty_part_and_a_name_with_no_room_goes_whole_and_alone() {
        assert_eq!(plan(&[], 64), vec![Vec::<Piece>::new()]);
        assert_eq!(
            plan(&[("@active".to_owned(), 0)], 64),
            vec![vec![piece("@active", 0, 0)]]
        );
        let long = "n".repeat(64);
        assert_eq!(
            plan(&[(long.clone(), 5)], 64),
            vec![vec![piece(&long, 0, 5)]]
        );
    }

    #[test]
    fn copies_are_grouped_counting_what_each_carries_besides_its_pages() {
        let copy = |at: usize| Copy {
            key: format!("k{at}"),
            accepted: None,
            promised: None,
            pages: pages(&[("v", b"1")]),
            part: 0,
            final_part: true,
        };
        let each = COPY + cost("k0", cost("v", 1));

        let groups = packed((0..10).map(copy).collect(), 3 * each);

        assert_eq!(
            groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![3, 3, 3, 1]
        );
        assert_eq!(packed(Vec::new(), 64), vec![Vec::<Copy>::new()]);
    }
}
