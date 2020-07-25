// Copyright IBM Corp. 2013,2018. All Rights Reserved.
// Node module: loopback-connector-oracle
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = require('rc')('loopback', {test: {oracle: {}}}).test.oracle;
config.maxConn = 64;

var db;

global.getDataSource = global.getSchema = function() {
  if (db) {
    return db;
  }
  db = new DataSource(require('../../'), config);
  db.log = function(a) {
    // console.log(a);
  };
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
};
