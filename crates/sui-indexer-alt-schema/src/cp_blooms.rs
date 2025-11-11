// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use diesel::prelude::*;
use sui_field_count::FieldCount;

use crate::schema::cp_blooms;

#[derive(Insertable, Selectable, Queryable, Debug, Clone, FieldCount)]
#[diesel(table_name = cp_blooms)]
pub struct StoredCpBlooms {
    pub cp_sequence_number: i64,
    pub bloom_filter: Vec<u8>,
}
