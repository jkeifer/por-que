EXPECTED_SCHEMAS = {
    'alltypes_plain_metadata': """Group(schema) {
  Column(id: INT32 OPTIONAL)
  Column(bool_col: BOOLEAN OPTIONAL)
  Column(tinyint_col: INT32 OPTIONAL)
  Column(smallint_col: INT32 OPTIONAL)
  Column(int_col: INT32 OPTIONAL)
  Column(bigint_col: INT64 OPTIONAL)
  Column(float_col: FLOAT OPTIONAL)
  Column(double_col: DOUBLE OPTIONAL)
  Column(date_string_col: BYTE_ARRAY OPTIONAL)
  Column(string_col: BYTE_ARRAY OPTIONAL)
  Column(timestamp_col: INT96 OPTIONAL)
}""",
    'delta_encoding_metadata': """Group(hive_schema) {
  Column(c_customer_sk: INT64 OPTIONAL)
  Column(c_current_cdemo_sk: INT64 OPTIONAL)
  Column(c_current_hdemo_sk: INT64 OPTIONAL)
  Column(c_current_addr_sk: INT64 OPTIONAL)
  Column(c_first_shipto_date_sk: INT64 OPTIONAL)
  Column(c_first_sales_date_sk: INT64 OPTIONAL)
  Column(c_birth_day: INT64 OPTIONAL)
  Column(c_birth_month: INT64 OPTIONAL)
  Column(c_birth_year: INT64 OPTIONAL)
  Column(c_customer_id: BYTE_ARRAY OPTIONAL)
  Column(c_salutation: BYTE_ARRAY OPTIONAL)
  Column(c_first_name: BYTE_ARRAY OPTIONAL)
  Column(c_last_name: BYTE_ARRAY OPTIONAL)
  Column(c_preferred_cust_flag: BYTE_ARRAY OPTIONAL)
  Column(c_birth_country: BYTE_ARRAY OPTIONAL)
  Column(c_email_address: BYTE_ARRAY OPTIONAL)
  Column(c_last_review_date: BYTE_ARRAY OPTIONAL)
}""",
    'nested_structs_metadata': """Group(schema) {
  Group(roll_num) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(PC_CUR) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(CVA_2012) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(CVA_2016) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(BIA_3) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(BIA_4) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(ACTUAL_FRONTAGE) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(ACTUAL_DEPTH) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(ACTUAL_LOT_SIZE) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(GLA) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(SOURCE_GLA) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(IPS_GLA) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(GLA_ALL) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(bia) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(EFFECTIVE_LOT_SIZE) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(effective_lot_area) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(EFFECTIVE_FRONTAGE) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(EFFECTIVE_DEPTH) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(rw_area_tot) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(effective_lot_sqft) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(dup) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(nonCTXT) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(vacantland) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(parkingbillboard) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(cvalte10) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(condootherhotel) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(calculated_lot_size) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(calculated_efflot_size) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(missingsite) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(missinggla) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(missingsitegla) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(actual_lot_size_sqft) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(lotsize_sqft) {
    Column(min: DOUBLE)
    Column(max: DOUBLE)
    Column(mean: DOUBLE)
    Column(count: INT64 [UINT_64])
    Column(sum: DOUBLE)
    Column(variance: DOUBLE)
  }
  Group(count) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
  Group(ul_observation_date) {
    Column(min: INT64 [TIMESTAMP_MICROS])
    Column(max: INT64 [TIMESTAMP_MICROS])
    Column(mean: INT64 [TIMESTAMP_MICROS])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [TIMESTAMP_MICROS])
    Column(variance: INT64 [TIMESTAMP_MICROS])
  }
  Group(ul_tz_offset_minutes_ul_observation_date) {
    Column(min: INT64 [INT_64])
    Column(max: INT64 [INT_64])
    Column(mean: INT64 [INT_64])
    Column(count: INT64 [UINT_64])
    Column(sum: INT64 [INT_64])
    Column(variance: INT64 [INT_64])
  }
}""",
}
