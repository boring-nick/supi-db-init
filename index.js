const path = require("path");
const { promisify } = require("util");
const readFile = promisify(require("fs").readFile);
const Maria = require("mariadb");

/**
 * Initializes database table definitions and initial data, per provided config.
 * @param {DatabaseInitializationConfiguration} config
 * @returns {Promise<void>}
 */
module.exports = async function initializeDatabase (config) {
	console.log("Script begin");

	const {
		auth = {},
		definitionFilePaths = [],
		initialDataFilePaths = [],
		meta = {},
	} = config;

	if (!auth) {
		throw new Error("No database access provided");
	}

	console.log("Starting database connection");

	let pool;
	try {
		pool = Maria.createPool(auth);
	}
	catch (e) {
		console.warn("Could not create database connection pool!");
		console.error(e);
		process.exit();
	}

	console.log("Database connection started");

	if (typeof meta.requiredMariaMajorVersion === "number") {
		const [{ version }] = await pool.query({
			sql: "SELECT VERSION() AS version"
		});

		const major = Number(version.split(".")[0]);
		if (Number.isNaN(major) || major < meta.requiredMariaMajorVersion) {
			throw new Error(`Your version of MariaDB is too old! Use at least ${meta.requiredMariaMajorVersion}.0 or newer. Your version: ${version}`);
		}
	}

	console.log("Starting SQL table definition script");

	let counter = 0;
	const definitionFolderPath = meta.definitionPath ?? "definitions";

	const dbExistsMap = new Map();

	for (const target of definitionFilePaths) {
		let content = null;

		const filePath = (sharedDefinitionNames.includes(target))
			? path.join(__dirname, "shared", "definitions", `${target}.sql`)
			: path.join(definitionFolderPath, `${target}.sql`);

		try {
			content = await readFile(filePath);
		}
		catch (e) {
			if (sharedDefinitionNames.includes(target)) {
				console.warn(`${target}.sql is not a shared definition file! Skipping...`, e);
			}
			else {
				console.warn(`An error occurred while reading definition file ${target}.sql! Skipping...`, e);
			}

			continue;
		}

		let string = null;
		const [database, type, name] = target.split("/");
		if (!dbExistsMap.has(database)) {
			if (database.includes(" ")) {
				console.error(`Invalid database name: ${database}`);
				process.exit(1);
			}

			const [{ dbExists }] = await pool.query(`
				SELECT 1 AS dbExists
				FROM INFORMATION_SCHEMA.SCHEMATA 
				WHERE SCHEMA_NAME = '${database}'
			`);

			if (!dbExists) {
				await pool.query(`
					 CREATE DATABASE IF NOT EXISTS \`${database}\`
					 CHARACTER SET = 'utf8mb4'
					 COLLATE = 'utf8mb4_general_ci';
				`);
			}
		}

		if (target.includes("tables")) {
			string = `Table ${database}.${name}`;
		}
		else if (target.includes("triggers")) {
			string = `Trigger ${database}.${name}`;
		}
		else {
			console.warn(`Unknown object type ${database}/${type}/${name}, skipping`);
			continue;
		 }

		let status = null;
		try {
			const operationResult = await pool.query({ sql: content.toString() });

			status = operationResult.warningStatus;
		}
		catch (e) {
			console.warn(`An error occurred while executing ${target}.sql! Skipping...`, e);
			continue;
		}

		if (status === 0) {
			counter++;
			console.log(`${string} created successfully`);
		}
		else {
			console.log(`${string} skipped - already exists`);
		}
	}

	console.log(`SQL table definition script succeeded.\n${counter} objects created`);

	console.log("Starting SQL table data initialization script");

	counter = 0;
	const dataFolderPath = meta.dataPath ?? "initial-data";

	for (const target of initialDataFilePaths) {
		let content = null;
		const filePath = (sharedInitialDataNames.includes(target))
			? path.join(__dirname, "shared", "initial-data", `${target}.sql`)
			: path.join(dataFolderPath, `${target}.sql`);

		try {
			content = await readFile(filePath);
		}
		catch (e) {
			if (sharedInitialDataNames.includes(target)) {
				console.warn(`${target}.sql is not a shared initial data file! Skipping...`, e);
			}
			else {
				console.warn(`An error occurred while reading initial data file ${target}.sql! Skipping...`, e);
			}

			continue;
		}

		const [database, table] = target.split("/");
		const rows = await pool.query({
			sql: `SELECT COUNT(*) AS Count FROM \`${database}\`.\`${table}\``
		});

		if (rows.Count > 0) {
			console.log(`Skipped initializing ${database}.${table} - table is not empty`);
			continue;
		}

		let duplicateRows = 0;
		let insertedRows = 0;
		try {
			const operationResult = await pool.query({ sql: content.toString() });
			duplicateRows = operationResult.warningStatus;
			insertedRows = operationResult.affectedRows;
		}
		catch (e) {
			console.warn(`An error occurred while executing ${target}.sql! Skipping...`, e);
			continue;
		}

		console.log(`${database}.${table} inserted ${insertedRows} rows (${duplicateRows} were already present)`);
		counter++;
	}

	console.log(`SQL table data initialization script succeeded.\n${counter} tables initialized`);

	await pool.end();

	console.log("Script end");
	process.exit();
};
