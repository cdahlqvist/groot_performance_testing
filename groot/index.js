
var _ = require('lodash');
var fs = require('fs');
var moment = require('moment');
var util = require('../../lib/util');

module.exports.init = function(esClient, parameters, driver_data) {
  var state = {};
  
  set_state_value('index', state, parameters, "*");
  set_state_value('period', state, parameters, 30);
  set_state_value('timeout', state, parameters, 30000);

  if (parameters && parameters['days']) {
    state.days = parse_days(parameters['days']);
  } else {
    state.days = parse_days('-1,0');
  }

  return state;
}

module.exports.lapd = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"size":0,"aggs":{"2":{"geohash_grid":{"field":"geo_location","precision":5}}},"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}}},"query":{"filtered":{"query":{"match_all":{}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"date_occ":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"size":0,"aggs":{"2":{"terms":{"field":"area_name","size":8,"order":{"_count":"desc"}},"aggs":{"3":{"terms":{"field":"crm_cd_desc","size":5,"order":{"_count":"desc"}}}}}},"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}}},"query":{"filtered":{"query":{"match_all":{}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"date_occ":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"size":0,"aggs":{"2":{"date_histogram":{"field":"date_occ","interval":"1M","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"crm_cd_desc","size":5,"order":{"_count":"desc"}}}}}},"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}}},"query":{"filtered":{"query":{"match_all":{}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"date_occ":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"size":0,"aggs":{"2":{"terms":{"field":"crm_cd_desc","size":8,"order":{"_count":"desc"}},"aggs":{"3":{"terms":{"field":"status_desc","size":5,"order":{"_count":"desc"}}}}}},"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}}},"query":{"filtered":{"query":{"match_all":{}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"date_occ":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"size":0,"aggs":{"2":{"terms":{"field":"location","size":5,"order":{"_count":"desc"}},"aggs":{"3":{"terms":{"field":"cross_street","size":5,"order":{"_count":"desc"}}}}}},"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}}},"query":{"filtered":{"query":{"match_all":{}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"date_occ":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 5 } );
    }

    result_callback( { result_code: 'OK', visualizations: 5 } );
  });
}

module.exports.elk = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"12h","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"geoip.country_name.raw","size":5,"order":{"1":"desc"}},"aggs":{"1":{"sum":{"field":"bytes"}}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"geohash_grid":{"field":"geoip.location","precision":2}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"cardinality":{"field":"clientip.raw"}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"agent.raw","size":10,"order":{"_count":"desc"}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"12h","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"response","size":5,"order":{"_count":"desc"}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"response","size":5,"order":{"_count":"desc"}},"aggs":{"3":{"significant_terms":{"field":"geoip.country_name.raw","size":3}}}}}},
    {"index":index,"ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}},"require_field_match":false,"fragment_size":2147483647},"size":500,"sort":[{"@timestamp":{"order":"desc","unmapped_type":"boolean"}}],"fields":["*","_source"],"script_fields":{},"fielddata_fields":["@timestamp"]},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"request.raw","size":10,"order":{"_count":"desc"}}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 8 } );
    }

    result_callback( { result_code: 'OK', visualizations: 8 } );
  });
}

module.exports.elk2 = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"12h","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"geoip.country_name.raw","size":5,"order":{"1":"desc"}},"aggs":{"1":{"sum":{"field":"bytes"}}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"geohash_grid":{"field":"geoip.location","precision":2}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"cardinality":{"field":"clientip.raw"}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"agent.raw","size":10,"order":{"_count":"desc"}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"12h","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"response","size":5,"order":{"_count":"desc"}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"response","size":5,"order":{"_count":"desc"}},"aggs":{"3":{"significant_terms":{"field":"geoip.country_name.raw","size":3}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"request.raw","size":10,"order":{"_count":"desc"}}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 7 } );
    }

    result_callback( { result_code: 'OK', visualizations: 7 } );
  });
}

// This is a artificial dashboard where the highlight and sort clauses have been removed from the search panel query.
module.exports.elk_revised = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"12h","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"geoip.country_name.raw","size":5,"order":{"1":"desc"}},"aggs":{"1":{"sum":{"field":"bytes"}}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"geohash_grid":{"field":"geoip.location","precision":2}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"cardinality":{"field":"clientip.raw"}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"agent.raw","size":10,"order":{"_count":"desc"}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"12h","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"response","size":5,"order":{"_count":"desc"}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"response","size":5,"order":{"_count":"desc"}},"aggs":{"3":{"significant_terms":{"field":"geoip.country_name.raw","size":3}}}}}},
    {"index":index,"ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":500,"fields":["*","_source"],"script_fields":{},"fielddata_fields":["@timestamp"]},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"request.raw","size":10,"order":{"_count":"desc"}}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 8 } );
    }

    result_callback( { result_code: 'OK', visualizations: 8 } );
  });
}

module.exports.accidentology = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}},"filter":{"bool":{"must":[{"query":{"query_string":{"query":"*","analyze_wildcard":true}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"1w","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}},"filter":{"bool":{"must":[{"query":{"query_string":{"query":"*","analyze_wildcard":true}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"geohash_grid":{"field":"location","precision":7}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"query":"*","analyze_wildcard":true}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"Vehicle 1 description","size":15,"order":{"_count":"desc"}},"aggs":{"3":{"terms":{"field":"Vehicle 2 Description","size":15,"order":{"_count":"desc"}},"aggs":{"4":{"terms":{"field":"Vehicle 3 Description","size":5,"order":{"_count":"desc"}}}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}},"filter":{"bool":{"must":[{"query":{"query_string":{"query":"*","analyze_wildcard":true}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"Address","size":10,"order":{"_count":"desc"}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"query":"*","analyze_wildcard":true}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"1w","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"Person 1 Status","size":5,"order":{"_count":"desc"}}}}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 5 } );
    }

    result_callback( { result_code: 'OK', visualizations: 5 } );
  });
}

module.exports.singapore_lta_advisories = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"geohash_grid":{"field":"geo","precision":7}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"message.raw","size":50,"order":{"_count":"desc"}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"1d","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"equipment_id","size":10,"order":{"_count":"desc"}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"equipment_id","size":10,"order":{"1":"desc"}},"aggs":{"1":{"cardinality":{"field":"message.raw"}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"equipment_id","size":10,"order":{"1":"asc"}},"aggs":{"1":{"cardinality":{"field":"message.raw"}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"cardinality":{"field":"equipment_id"}},"3":{"cardinality":{"field":"message.raw"}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 6 } );
    }

    result_callback( { result_code: 'OK', visualizations: 6 } );
  });
}

module.exports.singapore_lta_carparks = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"area.raw","size":5,"order":{"1":"desc"}},"aggs":{"1":{"sum":{"field":"available_lots"}},"3":{"terms":{"field":"name.raw","size":15,"order":{"1":"desc"}},"aggs":{"1":{"sum":{"field":"available_lots"}}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"1d","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"area.raw","size":5,"order":{"1":"desc"}},"aggs":{"1":{"avg":{"field":"available_lots"}}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"1":{"cardinality":{"field":"carpark_id"}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"name.raw","size":10,"order":{"1":"desc"}},"aggs":{"1":{"avg":{"field":"available_lots"}},"3":{"max":{"field":"available_lots"}},"4":{"min":{"field":"available_lots"}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"geohash_grid":{"field":"geo","precision":7},"aggs":{"1":{"avg":{"field":"available_lots"}}}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 5 } );
    }

    result_callback( { result_code: 'OK', visualizations: 5 } );
  });
}

module.exports.singapore_lta_incidents = function(esClient, state, driver_data, operation_parameters, result_callback) {
  var timeout = state.timeout;
  if(operation_parameters.timeout && util.is_integer(operation_parameters.timeout) && operation_parameters.timeout > 0) {
    timeout = operation_parameters.timeout;
  }

  var period = state.period;
  if(operation_parameters.period && util.is_integer(operation_parameters.period) && operation_parameters.period > 0) {
    period = operation_parameters.period;
  }

  var index = state.index;
  if(operation_parameters.index && (util.is_string(operation_parameters.index) || util.is_string_array(operation_parameters.index))) {
    index = operation_parameters.index;
  }

  var days = state.days;

  if(operation_parameters.days && util.is_string(operation_parameters.days)) {
    days = parse_days(operation_parameters.days);
  }

  var end_ts = _.random(days.start, days.end);
  var start_ts = end_ts - (period * 24 * 3600 * 1000);

  var bulk_body = [
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"terms":{"field":"type.raw","size":8,"order":{"1":"desc"}},"aggs":{"1":{"cardinality":{"field":"description.raw"}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"geohash_grid":{"field":"geo","precision":6},"aggs":{"1":{"cardinality":{"field":"description.raw"}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"12h","time_zone":"Europe/London","min_doc_count":1,"extended_bounds":{"min":start_ts,"max":end_ts}},"aggs":{"3":{"terms":{"field":"type.raw","size":8,"order":{"1":"desc"}},"aggs":{"1":{"cardinality":{"field":"description.raw"}}}}}}}},
    {"index":index,"search_type":"count","ignore_unavailable":true},
    {"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true}},"filter":{"bool":{"must":[{"query":{"query_string":{"analyze_wildcard":true,"query":"*"}}},{"range":{"@timestamp":{"gte":start_ts,"lte":end_ts,"format":"epoch_millis"}}}],"must_not":[]}}}},"size":0,"aggs":{"1":{"cardinality":{"field":"description.raw"}}}}
  ];

  esClient.msearch({
    preference: end_ts.toString(),
    body: bulk_body,
    requestTimeout: timeout
  }, function (err, resp) {
    if (err) {
      result_callback( { result_code: 'ERROR', visualizations: 4 } );
    }

    result_callback( { result_code: 'OK', visualizations: 4 } );
  });
}


function parse_days(str) {
  var interval, start, end;
  var absolute_day = /^(\d{4}-\d{2}-\d{2})$/;
  var absolute_day_interval = /^(\d{4}-\d{2}-\d{2}),(\d{4}-\d{2}-\d{2})$/;
  var relative_day_interval = /^([+-]?\d+),([+-]?\d+)$/;
 
  var match = absolute_day.exec(str);

  if (match) {
    start = moment(match[1]).utc().startOf('day').valueOf();
    end = moment(match[1]).utc().endOf('day').valueOf();

    interval = { start: start, end: end};
  } 

  match = absolute_day_interval.exec(str);

  if (match) {
    start = moment(match[1]).utc().startOf('day').valueOf();
    end = moment(match[2]).utc().endOf('day').valueOf();

    interval = { start: start, end: end};
  }

  match = relative_day_interval.exec(str);

  if (match) {
    start = moment().utc().startOf('day').add('days', parseInt(match[1])).valueOf();
    end = moment().utc().endOf('day').add('days', parseInt(match[2])).valueOf();

    interval = { start: start, end: end};
  }

  return interval;
}

function set_state_value(name, state, parameters, default_value) {
  if (parameters && parameters[name]) {
    state[name] = parameters[name];
  } else {
    state[name] = default_value;
  }
}

function load_string_file(str_file) {
  try {
    var file_data = fs.readFileSync(str_file, {encoding: 'utf-8'});
    var string_array = file_data.split(/\r?\n/);
    return string_array;
  }
  catch (e) {
    var logmsg = 'makelogs_kibana error: unable to load file ' + str_file + ': ' + e.message;
    util.log(logmsg);
    return undefined;
  }
}
