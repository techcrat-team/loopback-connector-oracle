// Copyright IBM Corp. 2015,2018. All Rights Reserved.
// Node module: loopback-connector-oracle
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';

var async = require('async');

module.exports = mixinMigration;

function mixinMigration(Oracle) {
  Oracle.prototype.showFields = function (model, cb) {
    var sql = 'SELECT column_name AS "column", data_type AS "type",' +
      ' data_length AS "length", nullable AS "nullable"' + // , data_default AS "Default"'
      ' FROM "SYS"."USER_TAB_COLUMNS" WHERE table_name=\'' +
      this.table(model) + '\'';
    this.execute(sql, function (err, fields) {
      if (err)
        return cb(err);
      else {
        fields.forEach(function (field) {
          field.type = mapOracleDatatypes(field.type);
        });
        cb(err, fields);
      }
    });
  };

  /**
 * Get the status of a table
 * @param {String} model The model name
 * @param {Function} cb The callback function
 */
  Oracle.prototype.getTableStatus = function (model, cb) {
    var fields, indexes;
    var self = this;

    this.showFields(model, function (err, data) {
      if (err) return cb(err);
      fields = data;

      self.showIndexes(model, function (err, data) {
        if (err) return cb(err);
        indexes = data;

        if (self.checkFieldAndIndex(fields, indexes))
          return cb(null, fields, indexes);
      });
    });
  };

  Oracle.prototype.showIndexes = function (model, cb) {
    var table = "'" + this.tableEscaped(model) + "'";
    var sql = `select u.INDEX_NAME, u.TABLE_NAME, u.UNIQUENESS, a.COLUMN_NAME, 
    a.column_position from ALL_IND_COLUMNS a inner join USER_INDEXES u on a.index_name = 
    u.index_name where u.TABLE_NAME =` + table;

    this.execute(sql, function (err, fields) {
      cb && cb(err, fields);
    });
  };




  /**
  * Perform autoupdate for the given models
  * @param {String[]} [models] A model name or an array of model names. If not
  * present, apply to all models
  * @param {Function} [cb] The callback function
  */
  Oracle.prototype.autoupdate = function (models, cb) {
    var self = this;
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);

    async.eachLimit(models, this.parallelLimit, function (model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function () {
          done(new Error('Model not found: ' + model));
        });
      }

      self.getTableStatus(model, function (err, fields, indexes) {
        self.discoverForeignKeys(self.table(model), {}, function (err, foreignKeys) {
          if (err) console.log('Failed to discover "' + self.table(model) +
            '" foreign keys', err);

          if (!err && fields && fields.length) {
            // if we already have a definition, update this table
            self.alterTable(model, fields, indexes, foreignKeys, function (err, result) {
              done(err);
            }, false);
          } else {
            // if there is not yet a definition, create this table
            self.createTable(model, function (err) {
              done(err);
            });
          }
        });
      });
    }, cb);
  };

  Oracle.prototype.alterTable = function (model, actualFields, actualIndexes, actualFks, done, checkOnly) {
    if ('function' == typeof actualFks && typeof done !== 'function') {
      checkOnly = done || false;
      done = actualFks;
    }
    var self = this;
    var statements = [];
    async.series([
      function (cb) {
        statements = self.getAddModifyColumns(model, actualFields);
        cb();
      },
      function (cb) {
        statements = statements.concat(self.getDropColumns(model, actualFields));
        cb();
      },
      // function (cb) {
      //   statements = statements.concat(self.addIndexes(model, actualIndexes));
      //   cb();
      // },
      // Commented temporarily to enable step by step development
      function (cb) {
        statements = statements.concat(self.dropForeignKeys(model, actualFks));
        cb();
      },
      // function (cb) {
      //   // get foreign keys to add, but when we are checking only
      //   if (checkOnly) {
      //     statements = statements.concat(self.getForeignKeySQL(model, self.getModelDefinition(model).settings.foreignKeys, actualFks));
      //   }
      //   cb();
      // }
    ], function (err, result) {
      if (err) done(err);

      // determine if there are column, index, or foreign keys changes (all require update)
      if (statements.length) {
        // get the required alter statements
        var alterStmt = self.getAlterStatement(model, statements);

        var stmtList = [alterStmt];

        // set up an object to pass back all changes, changes that have been run,
        // and foreign key statements that haven't been run
        var retValues = {
          statements: stmtList,
          query: stmtList.join('; '),
        };
        //console.log('statements', statements)
        // if we're running in read only mode OR if the only changes are foreign keys additions,
        // then just return the object directly
        if (checkOnly) {
          done(null, true, retValues);
        } else {
          // if there are changes in the alter statement, then execute them and return the object
          self.applySqlChanges(model, statements, function () {
            done(err, true, retValues);
          });
        }
      } else {
        done();
      }
    });
  };

  Oracle.prototype.getAlterStatement = function (model, statements) {
    return statements.length ?
      'ALTER TABLE ' + this.tableEscaped(model) + ' ' + statements.join(',\n') :
      '';
  };

  Oracle.prototype.dropForeignKeys = function (model, actualFks) {
    var self = this;
    var m = this.getModelDefinition(model);

    var fks = actualFks;
    var sql = [];
    var correctFks = m.settings.foreignKeys || {};

    // drop foreign keys for removed fields
    if (fks && fks.length) {
      var removedFks = [];
      fks.forEach(function (fk) {
        var needsToDrop = false;
        var newFk = correctFks[fk.fkName];
        if (newFk) {
          var fkCol = expectedColNameForModel(newFk.foreignKey, m);
          var fkEntity = self.getModelDefinition(newFk.entity);
          var fkRefKey = expectedColNameForModel(newFk.entityKey, fkEntity);
          var fkEntityName = (typeof newFk.entity === 'object') ? newFk.entity.name : newFk.entity;
          var fkRefTable = self.table(fkEntityName);

          needsToDrop = fkCol != fk.fkColumnName ||
            fkRefKey != fk.pkColumnName ||
            fkRefTable != fk.pkTableName;
        } else {
          needsToDrop = true;
        }

        if (needsToDrop) {
          sql.push('DROP CONSTRAINT ' + self.escapeName(fk.fkName));
          removedFks.push(fk); // keep track that we removed these
        }
      });

      // update out list of existing keys by removing dropped keys
      removedFks.forEach(function (k) {
        var index = actualFks.indexOf(k);
        if (index !== -1) actualFks.splice(index, 1);
      });
    }
    return sql;
  };

  Oracle.prototype.getForeignKeySQL = function (model, actualFks, existingFks) {
    var self = this;
    var m = this.getModelDefinition(model);
    var addFksSql = [];
    existingFks = existingFks || [];

    if (actualFks) {
      var keys = Object.keys(actualFks);
      for (var i = 0; i < keys.length; i++) {
        // all existing fks are already checked in Oracle.prototype.dropForeignKeys
        // so we need check only names - skip if found
        if (existingFks.filter(function (fk) {
          return fk.fkName === keys[i];
        }).length > 0) continue;
        var constraint = self.buildForeignKeyDefinition(model, keys[i]);
        if (constraint) {
          addFksSql.push(' ADD ' + constraint);
        }
      }
    }
    return addFksSql;
  };

  Oracle.prototype.buildForeignKeyDefinition = function (model, keyName) {
    var self = this;
    var definition = this.getModelDefinition(model);
    var fk = definition.settings.foreignKeys[keyName];
    if (fk) {
      // get the definition of the referenced object
      var fkEntityName = (typeof fk.entity === 'object') ? fk.entity.name : fk.entity;

      // verify that the other model in the same DB
      if (this._models[fkEntityName]) {
        return ' CONSTRAINT ' + self.escapeName(fk.name) +
          ' FOREIGN KEY (' + self.escapeName(expectedColNameForModel(fk.foreignKey, definition)) + ')' +
          ' REFERENCES ' + this.tableEscaped(fkEntityName) +
          '(' + self.escapeName(fk.entityKey) + ')';
      }
    }
    return '';
  };


  Oracle.prototype.applySqlChanges = function (model, pendingChanges, cb) {
    var self = this;
    if (pendingChanges.length) {
      var alterTable = (pendingChanges[0].substring(0, 10) !== 'DROP INDEX' && pendingChanges[0].substring(0, 6) !== 'CREATE' && pendingChanges[0].substring(0, 5) !== 'BEGIN');

      var thisQuery = alterTable ? 'ALTER TABLE ' + self.tableEscaped(model) : '';
      var ranOnce = false;
      pendingChanges.forEach(function (change) {
        if (ranOnce) {
          thisQuery = thisQuery + ' ';
        }
        thisQuery = thisQuery + ' ' + change;
        ranOnce = true;
      });
      self.execute(thisQuery, cb);
    }
  }

  Oracle.prototype.getAddModifyColumns = function (model, actualFields) {
    var sql = [];
    var self = this;
    sql = sql.concat(self.getColumnsToAdd(model, actualFields));
    sql = sql.concat(self.getPropertiesToModify(model, actualFields));
    return sql;
  };

  Oracle.prototype.getColumnsToAdd = function (model, actualFields) {
    var self = this;
    var m = self._models[model];
    var propNames = Object.keys(m.properties);
    var sql = [];
    propNames.forEach(function (propName) {
      if (self.id(model, propName)) {
        return;
      }
      var colName = expectedColNameForModel(propName, m);

      var found = self.searchForPropertyInActual(model,
        self.column(model, colName), actualFields);
      if (!found && self.propertyHasNotBeenDeleted(model, propName)) {
        sql.push(self.addPropertyToActual(model, propName));
      }
    });
    if (sql.length > 0) {
      sql = ['ADD', '(' + sql.join(',') + ')'];
    }
    return sql;
  };

  // Oracle.prototype.searchForPropertyInActual = function (model, propName,
  //   actualFields) {
  //   var self = this;
  //   var found = false;
  //   actualFields.forEach(function (f) {
  //     if (f.column === self.column(model, propName)) {
  //       found = f;
  //       return;
  //     }
  //   });
  //   return found;
  // }

  Oracle.prototype.getPropertiesToModify = function (model, actualFields) {
    var self = this;
    var sql = [];
    var m = self._models[model];
    var propNames = Object.keys(m.properties);
    var found;
    propNames.forEach(function (propName) {
      if (self.id(model, propName)) {
        return;
      }
      var colName = expectedColNameForModel(propName, m);

      found = self.searchForPropertyInActual(model, propName, actualFields);

      if (found && self.propertyHasNotBeenDeleted(model, propName)) {
        var column = self.columnEscaped(model, propName);
        var clause = '';
        if (datatypeChanged(colName, found)) {
          clause = column + ' ' +
            self.modifyDatatypeInActual(model, colName);
        }
        if (nullabilityChanged(colName, found)) {
          if (!clause) {
            clause = column;
          }
          clause = clause + ' ' +
            self.modifyNullabilityInActual(model, colName);
        }
        if (clause) {
          sql.push(clause);
        }
      }
    });

    if (sql.length > 0) {
      sql = ['MODIFY', '(' + sql.join(',') + ')'];
    }
    return sql;

    function datatypeChanged(propName, oldSettings) {
      var newSettings = m.properties[propName];
      if (!newSettings) {
        return false;
      }

      var oldType;
      if (hasLength(self.columnDataType(model, propName))) {
        oldType = oldSettings.type.toUpperCase() +
          '(' + oldSettings.length + ')';
      } else {
        oldType = oldSettings.type.toUpperCase();
      }

      return oldType !== self.columnDataType(model, propName);

      function hasLength(type) {
        var hasLengthRegex = new RegExp(/^[A-Z0-9]*\([0-9]*\)$/);
        return hasLengthRegex.test(type);
      }
    }

    function nullabilityChanged(propName, oldSettings) {
      var newSettings = m.properties[propName];
      if (!newSettings) {
        return false;
      }
      var changed = false;
      if (oldSettings.nullable === 'Y' && !self.isNullable(newSettings)) {
        changed = true;
      }
      if (oldSettings.nullable === 'N' && self.isNullable(newSettings)) {
        changed = true;
      }
      return changed;
    }
  };

  Oracle.prototype.modifyDatatypeInActual = function (model, propName) {
    var self = this;
    var sqlCommand = self.columnDataType(model, propName);
    return sqlCommand;
  };

  Oracle.prototype.modifyNullabilityInActual = function (model, propName) {
    var self = this;
    var sqlCommand = '';
    if (self.isNullable(self.getPropertyDefinition(model, propName))) {
      sqlCommand = sqlCommand + 'NULL';
    } else {
      sqlCommand = sqlCommand + 'NOT NULL';
    }
    return sqlCommand;
  };

  Oracle.prototype.getColumnsToDrop = function (model, actualFields) {
    var self = this;
    var sql = [];
    actualFields.forEach(function (actualField) {
      if (self.idColumn(model) === actualField.column) {
        return;
      }
      if (actualFieldNotPresentInModel(actualField, model)) {
        sql.push(self.escapeName(actualField.column));
      }
    });
    if (sql.length > 0) {
      sql = ['DROP', '(' + sql.join(',') + ')'];
    }
    return sql;

    function actualFieldNotPresentInModel(actualField, model) {
      return !(self.propertyName(model, actualField.column));
    }
  };

  /*!
   * Build a list of columns for the given model
   * @param {String} model The model name
   * @returns {String}
   */
  Oracle.prototype.buildColumnDefinitions = function (model) {
    var self = this;
    var sql = [];
    var pks = this.idNames(model).map(function (i) {
      return self.columnEscaped(model, i);
    });
    Object.keys(this.getModelDefinition(model).properties).
      forEach(function (prop) {
        var colName = self.columnEscaped(model, prop);
        sql.push('' + colName.toLowerCase() + ' ' + self.buildColumnDefinition(model, prop));
      });
    if (pks.length > 0) {
      sql.push('PRIMARY KEY(' + pks.join(',') + ')');
    }
    return sql.join(',\n  ');
  };

  /*!
   * Build settings for the model property
   * @param {String} model The model name
   * @param {String} propName The property name
   * @returns {*|string}
   */
  Oracle.prototype.buildColumnDefinition = function (model, propName) {
    var self = this;
    var result = self.columnDataType(model, propName);
    if (!self.isNullable(self.getPropertyDefinition(model, propName))) {
      result = result + ' NOT NULL';
    }
    return result;
  };

  Oracle.prototype._isIdGenerated = function (model) {
    var idNames = this.idNames(model);
    if (!idNames) {
      return false;
    }
    var idName = idNames[0];
    var id = this.getModelDefinition(model).properties[idName];
    var idGenerated = idNames.length > 1 ? false : id && id.generated;
    return idGenerated;
  };

  /**
   * Drop a table for the given model
   * @param {String} model The model name
   * @param {Function} [cb] The callback function
   */
  Oracle.prototype.dropTable = function (model, cb) {
    var self = this;
    var name = self.tableEscaped(model);
    var seqName = self.escapeName(self.table(model) + '_id_sequence');

    var count = 0;
    var dropTableFun = function (callback) {
      self.execute('DROP TABLE ' + name, function (err, data) {
        if (err && err.toString().indexOf('ORA-00054') >= 0) {
          count++;
          if (count <= 5) {
            self.debug('Retrying ' + count + ': ' + err);
            // Resource busy, try again
            setTimeout(dropTableFun, 200 * Math.pow(count, 2));
            return;
          }
        }
        if (err && err.toString().indexOf('ORA-00942') >= 0) {
          err = null; // Ignore it
        }
        callback(err, data);
      });
    };

    var tasks = [dropTableFun];
    if (this._isIdGenerated(model)) {
      tasks.push(
        function (callback) {
          self.execute('DROP SEQUENCE ' + seqName, function (err) {
            if (err && err.toString().indexOf('ORA-02289') >= 0) {
              err = null; // Ignore it
            }
            callback(err);
          });
        });
      // Triggers will be dropped as part the drop table
    }
    async.series(tasks, cb);
  };

  /**
   * Create a table for the given model
   * @param {String} model The model name
   * @param {Function} [cb] The callback function
   */
  Oracle.prototype.createTable = function (model, cb) {
    var self = this;
    var name = self.tableEscaped(model);
    var seqName = self.escapeName(self.table(model) + '_id_sequence');
    var triggerName = self.escapeName(self.table(model) + '_id_trigger');
    var idName = self.idColumnEscaped(model);

    var tasks = [
      function (callback) {
        self.execute('CREATE TABLE ' + name + ' (\n  ' +
          self.buildColumnDefinitions(model) + '\n)', callback);
      }];


    if (this._isIdGenerated(model)) {
      tasks.push(
        function (callback) {
          self.execute('CREATE SEQUENCE ' + seqName +
            ' START WITH 1 INCREMENT BY 1 CACHE 100', callback);
        });

      tasks.push(
        function (callback) {
          self.execute('CREATE OR REPLACE TRIGGER ' + triggerName +
            ' BEFORE INSERT ON ' + name + ' FOR EACH ROW\n' +
            'WHEN (new.' + idName + ' IS NULL)\n' +
            'BEGIN\n' +
            '  SELECT ' + seqName + '.NEXTVAL INTO :new.' +
            idName + ' FROM dual;\n' +
            'END;', callback);
        });

      tasks.push(
        function (callback) {
          self.execute(self.createIndexes(model), callback);
        });
    }

    async.series(tasks, cb);
  };

  /*!
    * Find the column type for a given model property
    *
    * @param {String} model The model name
    * @param {String} property The property name
    * @returns {String} The column type
    */
  Oracle.prototype.columnDataType = function (model, property) {
    var columnMetadata = this.columnMetadata(model, property);
    var colType = columnMetadata && columnMetadata.dataType;
    if (colType) {
      colType = colType.toUpperCase();
    }
    var prop = this.getModelDefinition(model).properties[property];
    if (!prop) {
      return null;
    }

    var colLength = columnMetadata && columnMetadata.dataLength || prop.length;
    if (colType) {
      return colType + (colLength ? '(' + colLength + ')' : '');
    }

    switch (prop.type.name) {
      default:
      case 'String':
      case 'JSON':
        return 'VARCHAR2' + (colLength ? '(' + colLength + ')' : '(1024)');
      case 'Text':
        return 'VARCHAR2' + (colLength ? '(' + colLength + ')' : '(1024)');
      case 'Number':
        return 'NUMBER';
      case 'Date':
        return 'DATE';
      case 'Timestamp':
        return 'TIMESTAMP(3)';
      case 'Boolean':
        return 'CHAR(1)'; // Oracle doesn't have built-in boolean
    }
  };

  function mapOracleDatatypes(typeName) {
    // TODO there are a lot of synonymous type names that should go here--
    // this is just what i've run into so far
    switch (typeName) {
      case 'int4':
        return 'NUMBER';
      case 'bool':
        return 'CHAR(1)';
      default:
        return typeName;
    }
  }

  Oracle.prototype.isActual = function (models, cb) {
    var ok = false;
    var self = this;
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);
    async.each(models, function (model, done) {
      self.getTableStatus(model, function (err, fields, indexes) {
        self.discoverForeignKeys(self.table(model), {}, function (err, foreignKeys) {
          if (err) console.log('Failed to discover "' + self.table(model) +
            '" foreign keys', err);

          self.alterTable(model, fields, indexes, foreignKeys, function (err, needAlter) {
            if (err) {
              return done(err);
            } else {
              ok = ok || needAlter;
              done(err);
            }
          }, true);
        });
      });
    }, function (err) {
      if (err) {
        return err;
      }
      cb(null, !ok);
    });
  };

  Oracle.prototype.toModelIndexName = function (model, indexName) {
    var table = this.tableEscaped(model);
    return indexName.replace(table + '_', '');
  }

  Oracle.prototype.toActualIndexName = function (model, indexName) {
    var table = this.tableEscaped(model);
    return indexName.replace('idx_', 'idx_' + table + '_');
  }

  Oracle.prototype.addIndexes = function (model, actualIndexes) {
    var self = this;
    var m = this._models[model];
    var idName = this.idName(model);
    var indexNames = m.settings.indexes ? Object.keys(m.settings.indexes).filter(function (name) {
      return !!m.settings.indexes[name];
    }) : [];

    var propNames = Object.keys(m.properties).filter(function (name) {
      return !!m.properties[name];
    });
    var ai = {};
    var sql = [];

    sql.push(`BEGIN \n`);

    if (actualIndexes) {
      actualIndexes.forEach(function (i) {
        var name = i.INDEX_NAME;
        if (!ai[name]) {
          ai[name] = {
            info: i,
            columns: [],
          };
        }
        ai[name].columns[i.COLUMN_POSITION - 1] = i.COLUMN_NAME;
      });
    }

    var aiNames = Object.keys(ai);
    // remove indexes
    aiNames.forEach(function (indexName) {
      if (indexName.substr(0, 3) === 'SYS') {
        return;
      }
      var actualIndexName = indexName;
      indexName = self.toModelIndexName(model, indexName);

      if (indexNames.indexOf(indexName) === -1 && !m.properties[indexName] ||
        m.properties[indexName] && !m.properties[indexName].index) {
        sql.push(`EXECUTE IMMEDIATE 'DROP INDEX "${actualIndexName}"'; \n`);
      } else {
        // first: check single (only type and kind)
        if (m.properties[indexName] && !m.properties[indexName].index) {
          // TODO
          return;
        }
        // second: check multiple indexes
        var orderMatched = true;
        if (indexNames.indexOf(indexName) !== -1) {
          if (m.settings.indexes[indexName].columns) {
            // check if indexes are configured as "columns"
            m.settings.indexes[indexName].columns.split(/,\s*/).forEach(function (columnName, i) {
              if (ai[actualIndexName].columns[i] !== columnName) {
                orderMatched = false;
              }
            });
          } else if (m.settings.indexes[indexName].keys) {

            // if indexes are configured as "keys"
            var index = 0;
            for (var key in m.settings.indexes[indexName].keys) {
              //var sortOrder = m.settings.indexes[indexName].keys[key];
              if (ai[actualIndexName].columns[index] !== key) {
                orderMatched = false;
                break;
              }
              index++;
            }
            // if number of columns differ between new and old index
            if (index !== ai[actualIndexName].columns.length) {
              orderMatched = false;
            }
          }
        }
        if (!orderMatched) {

          sql.push(`EXECUTE IMMEDIATE 'DROP INDEX "${actualIndexName}"'; \n`);
          delete ai[actualIndexName];
        }
      }
    });

    // add single-column indexes
    // propNames.forEach(function (propName) {
    //   var found = ai[propName] && ai[propName].info;
    //   console.log('found', propName)
    //   if (!found) {
    //     var tblName = self.tableEscaped(model);
    //     var i = m.properties[propName].index;
    //     if (!i) {
    //       return;
    //     }
    //     var unique = false;

    //     if (i.unique) {
    //       unique = true;
    //     }
    //     var colName = expectedColNameForModel(propName, m);
    //     // var pName = self.client.escapeId(colName);
    //     //var name = colName + '_' + kind + '_' + type + '_idx';
    //     var table = self.table(model);
    //     var name = 'idx_' + table + '_' + colName;
    //     if (i.name) {
    //       name = i.name;
    //     }
    //     // self._idxNames[model].push(name);
    //     //var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + 'INDEX ' + name + ' ON ' + tblName + ' (\"' + prop + '\");';
    //     var cmd = `EXECUTE IMMEDIATE 'CREATE ${(unique ? 'UNIQUE ' : '')}INDEX "${name}" ON ${tblName} ("${prop}")' ; \n`;

    //     sql.push(cmd);
    //   }
    // });



    // add multi-column indexes
    indexNames.forEach(function (indexName) {
      var indexNameInModel = self.toActualIndexName(model, indexName);

      var found = ai[indexNameInModel] && ai[indexNameInModel].info;
      // console.log('indexName', indexNameInModel)
      // console.log('ai[indexName]', ai[indexNameInModel])

      if (!found) {
        console.log('NOT FOUND', indexNameInModel)
        var tblName = self.tableEscaped(model);
        var i = m.settings.indexes[indexName];
        var unique = false;
        if (i.unique) {
          unique = true;
        }
        var splitcolumns = [];
        var columns = [];
        var name = '';
        var indexColumns = '';

        // if indexes are configured as "keys"
        if (i.keys) {
          for (var key in i.keys) {
            splitcolumns.push(key);
          }
        } else if (i.columns) {
          splitcolumns = i.columns.split(',');
        }

        splitcolumns.forEach(function (elem, ind) {
          var trimmed = elem.trim();
          indexColumns = indexColumns ? indexColumns + ", " : '';
          indexColumns = indexColumns + '\"' + trimmed + '\"';
          name = name ? name + '_' + trimmed : trimmed;
          columns.push(trimmed);
        });


        name = name.charAt(name.length - 1) === '_' ? name.slice(0, -1) : name;
        // name += kind + '_' + type + '_idx';

        var table = self.table(model) + '_';

        name = 'idx_' + table + name;

        // self._idxNames[model].push(name);

        // var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + 'INDEX \"' + name + '\" ON ' + tblName + ' (' + indexColumns + ');';
        var cmd = `EXECUTE IMMEDIATE 'CREATE ${(unique ? 'UNIQUE ' : '')}INDEX "${name}" ON ${tblName} (${indexColumns})'; \n`;

        sql.push(cmd);
      }
    });
    sql.push(`END;`);

    return sql;
  };

  function expectedColNameForModel(propName, modelToCheck) {

    var oracle = modelToCheck.properties[propName].oracle;
    if (typeof oracle === 'undefined') {
      return propName;
    }
    var colName = oracle.columnName;

    if (typeof colName === 'undefined') {
      return propName;
    }
    return colName;
  }

  Oracle.prototype.createIndexes = function (model) {
    var self = this;
    var sql = [];
    sql.push(`BEGIN \n`);
    // Declared in model index property indexes.
    // Object.keys(this._models[model].properties).forEach(function (prop) {
    //   var i = self._models[model].properties[prop].index;
    //   if (i) {
    //     sql.push(self.singleIndexSettingsSQL(model, prop));
    //   }
    // });

    // Settings might not have an indexes property.
    var dxs = this._models[model].settings.indexes;
    if (dxs) {
      Object.keys(this._models[model].settings.indexes).forEach(function (prop) {
        sql.push(self.indexSettingsSQL(model, prop));
      });
    }
    sql.push(`END;`);
    var sqlString = '';
    sql.forEach(function (cmd) {
      sqlString = sqlString ? sqlString + '\n' + cmd : cmd;
    });
    return sqlString;
  };



  Oracle.prototype.singleIndexSettingsSQL = function (model, prop) {
    // Recycled from alterTable single indexes above, more or less.
    var tblName = this.tableEscaped(model);
    var i = this._models[model].properties[prop].index;
    var unique = false;
    if (i.unique) {
      unique = true;
    }
    // var name = prop + '_' + kind + '_' + type + '_idx';
    //var name = 'idx_' + tblName + '_' + prop;
    var name = (this.table(model) + '_' + prop);

    // if (i.name) {
    //   name = i.name;
    // }
    // this._idxNames[model].push(name);
    // var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + 'INDEX \"' + name + '\" ON ' + tblName + ' (\"' + prop + '\");';
    var cmd = `EXECUTE IMMEDIATE 'CREATE ${(unique ? 'UNIQUE ' : '')}INDEX "${name}" ON ${tblName} ("${prop}")'; \n`;

    return cmd;
  };

  Oracle.prototype.indexSettingsSQL = function (model, prop) {
    // Recycled from alterTable multi-column indexes above, more or less.
    var tblName = this.tableEscaped(model);
    var i = this._models[model].settings.indexes[prop];
    var unique = false;
    if (i.options && i.options.unique) {
      unique = true;
    }
    var splitcolumns = [];
    var columns = [];
    var name = '';
    var indexColumns = '';

    // if indexes are configured as "keys"
    if (i.keys) {
      for (var key in i.keys) {
        splitcolumns.push(key);
      }
    } else if (i.columns) {
      splitcolumns = i.columns.split(',');
    }

    splitcolumns.forEach(function (elem, ind) {
      var trimmed = elem.trim();
      indexColumns = indexColumns ? indexColumns + ", " : '';
      indexColumns = indexColumns + '\"' + trimmed + '\"';
      name = name ? name + '_' + trimmed : trimmed;
      columns.push(trimmed);
    });


    name = name.charAt(name.length - 1) === '_' ? name.slice(0, -1) : name;
    // name += kind + '_' + type + '_idx';

    var table = this.tableEscaped(model) + '_';

    //name = 'idx_' + table + name;
    name = (this.table(model) + '_' + prop);
    // self._idxNames[model].push(name);

    // var cmd = 'CREATE ' + (unique ? 'UNIQUE ' : '') + 'INDEX \"' + name + '\" ON ' + tblName + ' (' + indexColumns + ');';
    var cmd = `EXECUTE IMMEDIATE 'CREATE ${(unique ? 'UNIQUE ' : '')}INDEX "${name}" ON ${tblName} (${indexColumns})'; \n`;

    return cmd;
  };


  /**
    * Perform createForeignKeys for the given models
    * @param {String[]} [models] A model name or an array of model names.
    * If not present, apply to all models
    * @param {Function} [cb] The callback function
    */
  Oracle.prototype.createForeignKeys = function (models, cb) {
    var self = this;

    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);

    async.eachSeries(models, function (model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function () {
          done(new Error('Model not found: %s', model));
        });
      }

      self.getTableStatus(model, function (err, fields, indexes) {
        self.discoverForeignKeys(self.table(model), {}, function (err, foreignKeys) {
          if (err) console.log('Failed to discover "' + self.table(model) +
            '" foreign keys', err);

          if (!err && fields && fields.length) {
            var fkSQL = self.getForeignKeySQL(model,
              self.getModelDefinition(model).settings.foreignKeys,
              foreignKeys);
            self.addForeignKeys(model, fkSQL, function (err, result) {
              done(err);
            });
          } else {
            done(err);
          }
        });
      });
    }, function (err) {
      return cb(err);
    });
  };

  Oracle.prototype.addForeignKeys = function (model, fkSQL, cb) {
    var self = this;
    var m = this.getModelDefinition(model);

    if ((!cb) && ('function' === typeof fkSQL)) {
      cb = fkSQL;
      fkSQL = undefined;
    }

    if (!fkSQL) {
      var newFks = m.settings.foreignKeys;
      if (newFks)
        fkSQL = self.getForeignKeySQL(model, newFks);
    }
    if (fkSQL && fkSQL.length) {
      async.eachSeries(
        fkSQL,
        function (fkSQLStatement, callback) {
          self.applySqlChanges(model, [fkSQLStatement], function (err, result) {
            if (err) {
              callback(err);
            }
            else {
              console.log('Created foreign keys for ' + fkSQLStatement);
              callback(null, result);
            }
          });
        },
        function (error, data) {
          cb(error, data);
        });
    } else cb(null, {});
  };


}
