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
		definitionFiles = [],
		initialDataFiles = [],
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

	for (const target of definitionFiles) {
		let content = null;

		const filePath = path.join(definitionFolderPath, `${target}.sql`);
		try {
			content = await readFile(filePath);
		}
		catch (e) {
			console.warn(`An error occurred while reading ${target}.sql! Skipping...`, e);
			continue;
		}

		let string = null;
		const [database, type, name] = target.split("/");
		if (type === "database") {
			string = `Database ${database}`;
		}
		else if (target.includes("tables")) {
			string = `Table ${database}.${name}`;
		}
		else if (target.includes("triggers")) {
			string = `Trigger ${database}.${name}`;
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

	for (const target of initialDataFiles) {
		let content = null;
		const filePath = path.join(dataFolderPath, `${target}.sql`);

		try {
			content = await readFile(filePath);
		}
		catch (e) {
			console.warn(`An error occurred while reading ${target}.sql! Skipping...`, e);
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
			console.log(`${database}.${table} initial data inserted successfully`);
		}
		else if (status === 1) {
			counter++;
			console.log(`${database}.${table} initial data inserted successfully, some rows were skipped as they already existed before`);
		}
		else {
			console.log(`${database}.${table} initial data skipped - error occurred`);
		}
	}

	console.log(`SQL table data initialization script succeeded.\n${counter} tables initialized`);

	pool.end();

	console.log("Script end");
	process.exit();
};
