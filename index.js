'use strict';

module.exports = (r, options) => {
	if(typeof r !== 'function')
		throw new TypeError(`r must be a RethinkDB instance. Passed instances is a '${typeof r}'`);

	r.init = async (connection, schema) => {
		if(typeof connection.db !== 'string')
			throw new TypeError('Connection is not an object or doesn`t have a db property.');

		if(!Array.isArray(schema))
			throw new TypeError('Schema must be an array.');

		var conn = null;

		try {
			conn = await r.connect(connection);
			await lock.set(conn.db);
		}
		catch(error) {
			await lock.unset(conn.db);
			let m = 'Cannot establish connection';
			console.error(m, error);
			throw new Error(m);
		}

		try {
			await createDB(conn, conn.db);
			await checkDuplicateDBs(conn);
		}
		catch(error) {
			await lock.unset(conn.db);
			let m = 'Could not initialize DB';
			console.error(m, error);
			throw new Error(m);
		}

		try {
			let res = await createTables(conn, conn.db, schema);
			debug('Initialized tables', res);
			await lock.unset(conn.db);
			return conn;
		}
		catch(error) {
			await lock.unset(conn.db);
			let m = 'Could not create tables';
			console.error(m, error);
			throw new Error(m);
		}
	};

	options = Object.assign({debug: true}, options);

	const lock = require('./lock');
	const pick = (obj, fields) => fields.reduce((o, f) => obj[f] ? Object.assign(o, {[f]: obj[f]}) : o, {});
	const debug = options.debug ? (...args) => console.log.apply(console, args) : (...args) => args;

	const dropTable = (conn, db, table) => r.db(db).tableDrop(table).run(conn);
	const dropDB = (conn, db) => r.dbDrop(db).run(conn);
	const dbIsReady = (conn, db, table = null) => {
		let q = r.db(db);

		if(table)
			q = q.table(table);

		return q.wait().run(conn);
	};

	const createDB = async (conn, db = conn.db) => {
		try {
			let res = await r.dbList().run(conn).then(() => r.dbCreate(db).run(conn));
			debug('Created DB', ...Object.values(res.config_changes[0].new_val));
		}
		catch(error) {
			if(!error.message.includes('already exists')) {
				let m = `Cannot create DB '${db}' due to unhandled error`;
				console.error(m, error);
				throw new Error(m);
			}
			else
				debug(`DB '${db}' already exists.`);
		}

		return db;
	};

	const dbExists = (conn, db = conn.db) =>
		r.db('rethinkdb')
		.table('db_config')
		.filter({name: db})
		.count()
		.run(conn)
		.then(count => count > 0);

	const checkDuplicateDBs = (conn, db = conn.db) =>
		r.db(db)
		.tableList()
		.run(conn)
		.then(() => false)
		.catch(error => {
			if(error.message.includes('does not exist'))
				return false;

			return error.message.includes('ambiguous') || error;
		})
		.then(async duplicates => {
			if(duplicates === true) { // strict since it could could be an Error
				console.error(`Oh no, there are more than one DB named '${db}'!\nFixing...`);
				try {
					await fixDB(conn, db)
					.then(() => console.error(`Succesfully fixed duplicate DBs '${db}'.`));
				}
				catch(error) {
					let m = `Something wrong happened during fixing of duplicate DBs '${db}':`;
					console.error(m, error);
					throw new Error(m);
				}
			}
			else if(duplicates) { // error
				let m = `Something wrong happened during checking for duplicate DBs '${db}':`;
				console.error(m, error);
				throw new Error(m);
			}

			return false;
		});

	const dbIsNotEmpty = (conn, db = conn.db) =>
		r.db('rethinkdb')
		.table('table_config')
		.filter({db: db})
		.count()
		.run(conn)
		.then(count => count > 0);

	const checkDuplicateTables = (conn, db, [table, settings]) =>
		r.db(db)
		.table(table)
		.count()
		.run(conn)
		.then(() => false)
		.catch(error => {
			if(error.message.includes('does not exist'))
				return false;

			return error.message.includes('ambiguous') || error;
		})
		.then(async duplicates => {
			if(duplicates === true) { // strict since it could could be an Error
				console.error(`Oh no, there are more than one table named '${db}.${table}'!\nFixing...`);
				try {
					await fixTable(conn, db, table, settings)
					.then(() => console.error(`Succesfully fixed duplicate tables '${db}.${table}'.`));
				}
				catch(error) {
					let m = `Something wrong happened during fixing of duplicate tables '${db}.${table}':`;
					console.error(m, error);
					throw new Error(m);
				}
			}
			else if(duplicates) { // error
				let m = `Something wrong happened during checking for duplicate tables '${db}.${table}':`;
				console.error(m, error);
				throw new Error(m);
			}

			return false;
		});

	const tableExists = (conn, db, table) =>
		r.db('rethinkdb')
		.table('table_config')
		.filter({db: db, name: table})
		.count()
		.run(conn)
		.then(count => count > 0);

	const tableIsNotEmpty = async (conn, db, table) => {
		await dbIsReady(conn, db, table);

		return r.db(db)
		.table(table)
		.count()
		.run(conn)
		.then(count => count > 0 && table)
		.catch(error => {
			if(error.message.includes('does not exist'))
				return false;
			else
				throw error;
		});
	};

	const moveData = async (conn, from, to) => {
		await dbIsReady(from.db, from.table);
		await dbIsReady(to.db, to.table);

		return r.db(from.db)
		.table(from.table)
		.run(conn)
		.then(cursor => cursor.toArray())
		.then(items =>
			r.db(to.db)
			.table(to.table)
			.insert(items, {confilct: (id, old) => old}) // TODO: think about resolving conflicts later
			.run(conn)
		);
	};

	const moveTable = (conn, from, to) =>
		tableExists(conn, to.db, to.table)
		.then(exists => {
			if(exists) {
				debug(`Table ${to.db}.${to.table} already exists, moving data`);
				return moveData(conn, from, to) // TODO: actually move data, not copy. Then check if the old table is empty
				.then(() => dropTable(conn, from.db, from.table))
				.then(() => debug(`Dropped table ${from.db}.${from.table}`))
				.then(() => to);
			}
			else {
				debug(`Table ${to.db}.${to.table} doesn't exist, renaming ${from.db}.${from.table}`);
				return r.db('rethinkdb')
					.table('table_config')
					.filter({db: from.db, name: from.table})
					.update({db: to.db, name: to.table})
					.run(conn)
					.then(() => debug(`Renamed table ${from.db}.${from.table} into ${to.db}.${to.table}`))
					.then(() => to);
			}
		});

	const moveTables = (conn, dbFrom, dbTo) =>
		createDB(conn, dbTo)
		.catch(error => {
			console.error(`DB '${dbTo}' doesn't exist and couldn't be created, so there's no DB to move tables into.`, error);
			return false;
		})
		.then(async created => {
			if(!created)
				return false;

			await dbIsReady(conn, dbFrom);

			return r.db(dbFrom)
			.tableList()
			.run(conn)
			.then(tables => tables.map(t => tableIsNotEmpty(conn, dbFrom, t)))
			.then(promises => Promise.all(promises))
			.then(tables => tables.filter(t => t)) // here we return all non-empty tables
			.then(tables => tables.map(t => moveTable(conn, {db: dbFrom, table: t}, {db: dbTo, table: t})))
			.then(promises => Promise.all(promises));
		});

	const fixDB = (conn, dbname = conn.db) =>
		r.db('rethinkdb')
		.table('db_config')
		.filter({name: dbname})
		.run(conn)
		.then(cursor => cursor.toArray())
		.then(dbs => {
			debug(`Got ${dbs.length} duplicates of ${dbname} database`);
			return dbs;
		})
		.then(dbs =>
			dbs.map(db => {
				let backupDB = `${db.name}_${db.id.split('-')[0]}`;

				return r.db('rethinkdb')
				.table('db_config')
				.get(db.id)
				.update({name: backupDB})
				.run(conn)
				.then(() => debug(`Renamed database ${db.name}/${db.id} into ${backupDB}`))
				.then(() => dbIsNotEmpty(conn, backupDB))
				.then(notEmpty => {
					if(notEmpty) {
						debug(`Database ${backupDB} (${db.id}) is not empty. Preparing to move tables.`);
						return backupDB;
					}
					else {
						return dropDB(conn, backupDB)
						.then(() => {
							debug(`Dropped empty duplicate database ${backupDB} (${db.name} / ${db.id})`);
							return null;
						})
						.catch(error => {
							if(!(error.message || error.msg).includes('does not exist'))
								throw new Error(error);

							return null;
						});
					}
				})
				.then(backupDB => ({origin: db.name, backup: backupDB}));
			})
		)
		.then(promises => Promise.all(promises))
		.then(dbs => dbs.filter(db => db.backup)) // if there's a backup, then DB wasn't empty, so let's move data into a proper DB
		.then(dbs => dbs.map(db => moveTables(conn, db.backup, db.origin)))
		.then(promises => Promise.all(promises))
		.then(() => dbExists(conn, dbname))
		.then(dbExists => dbExists ||
			createDB(conn, dbname)
			.then(() => checkDuplicateDBs(conn, dbname))
		);

	const fixTable = (conn, db, tablename, settings) =>
		r.db('rethinkdb')
		.table('table_config')
		.filter({db: db, name: tablename})
		.run(conn)
		.then(cursor => cursor.toArray())
		.then(tables => {
			debug(`Got ${tables.length} duplicates of ${db}.${tablename} table`);
			return tables;
		})
		.then(tables =>
			tables.map(table => {
				let backupTable = `${table.name}_${table.id.split('-')[0]}`;

				return r.db('rethinkdb')
				.table('table_config')
				.get(table.id)
				.update({name: backupTable})
				.run(conn)
				.then(() => debug(`Renamed table ${db}.${table.name} (${table.id}) into ${backupTable}`))
				.then(() => tableIsNotEmpty(conn, db, backupTable))
				.then(notEmpty => {
					if(notEmpty) {
						debug(`Table ${db}.${backupTable} (${table.id}) is not empty. Preparing to move data.`);
						return backupTable;
					}
					else {
						return dropTable(conn, db, backupTable)
						.then(() => {
							debug(`Dropped empty duplicate table '${db}.${backupTable}' (${table.name} / ${table.id})`);
							return null;
						})
						.catch(error => {
							if(!(error.message || error.msg).includes('does not exist'))
								throw new Error(error);

							return null;
						});
					}
				})
				.then(backupTable => ({origin: table.name, backup: backupTable}));
			})
		)
		.then(promises => Promise.all(promises))
		.then(tables => tables.filter(table => table.backup)) // if there's a backup, then table wasn't empty, so let's move data
		.then(tables => tables.map(table => moveTable(conn, {db: db, table: table.backup}, {db: db, table: table.origin})))
		.then(promises => Promise.all(promises))
		.then(() => tableExists(conn, db, tablename))
		.then(exists => exists ||
			createTable(conn, db, tablename, settings)
			.then(() => checkDuplicateTables(conn, db, tablename, settings))
		);

	const createIndexes = (table, db, conn) => Promise.all(
		table.indexes.map(async index => {
			if(!index.name && typeof index !== 'string')
				throw new TypeError(`Index entry in table (${table.name}) entry must be 'Object' (with '.name' property) or 'String'`);

			let opts = typeof index === 'string'
				? [index]
				: [
					index.name,
					index.indexFunction || pick(index, ['multi', 'geo']),
					index.indexFunction ? pick(index, ['multi', 'geo']) : undefined
				];

			await dbIsReady(conn, db, table.name);

			return r.db(db)
			.table(table.name)
			.indexCreate(...opts)
			.run(conn)
			.catch(error => {
				if((error.msg || error.message).includes('already exists')) {
					debug(`Indexes for table ${db}.${table.name} already exist`);
					return true;
				}
				else
					throw error;
			});
		})
	);

	const createTable = async (conn, db, table, settings = {}) => {
		if(!table.name && typeof table !== 'string')
			throw new TypeError('Table entry must be an Object (with `.name` property) or a String');

		let opts = typeof table === 'string'
			? [table, settings]
			: [table.name, pick(Object.assign({}, table, settings), ['primaryKey', 'durability', 'shards', 'replicas', 'primaryReplicaTag'])];

		await dbIsReady(conn, db);

		return r.db(db)
		.tableCreate(...opts)
		.run(conn)
		.then(() => debug(`Created table ${db}.${opts[0]}`))
		.catch(error => {
			if(error.message.includes('already exists')) {
				debug(`Table ${db}.${opts[0]} already exists`);
				return `${db}.${opts[0]}`;
			}
			else
				throw error;
		})
		.then(
			() => checkDuplicateTables(conn, db, opts),
			error => {
				let m = `Couldn't initialize table '${db}.${opts[0]}'`;
				console.error(m, error);
				throw new Error(m);
			}
		)
		.then(() => table.indexes && table.indexes.length
			? createIndexes(Object.assign(table, {indexes: table.indexes}), db, conn)
			: null
		)
		.then(
			() => `${db}.${opts[0]}` + (table.indexes ? ` with ${table.indexes.length} indexes` : ''),
			error => {
				debug(`Created table ${db}.${opts[0]}, couldn't create indexes due to error: ${error.message}`);
				return `${db}.${opts[0]}, failed with indexes (${table.indexes && table.indexes.length})`;
			}
		);
	};

	const createTables = (conn, db, tables) =>
		Promise.all(tables.map(table =>	createTable(conn, db, table)));

	return r.init;
};
