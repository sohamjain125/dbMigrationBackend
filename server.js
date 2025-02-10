const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const xlsx = require('xlsx');
const fs = require('fs');
const cors = require('cors');
const sql = require('mssql/msnodesqlv8');

const app = express();
app.use(cors());
app.use(express.json());

// Configure file upload
const upload = multer({ dest: 'uploads/' });

const config = {
    driver: 'msnodesqlv8',
    connectionString: 'Driver={SQL Server};Server=localhost;Database=StudentPortalDb;Trusted_Connection=yes;'
};
async function testConnection() {
    try {
        let pool = await sql.connect(config);
        console.log('Connected to SQL Server');

        // Test query
        const result = await pool.request().query('SELECT @@VERSION as version');
        console.log('SQL Server version:', result.recordset[0].version);
    } catch (err) {
        console.error('SQL Connection Error:', err);
        console.error('Error details:', {
            code: err.code,
            number: err.number,
            state: err.state,
            message: err.message
        });
    }
}
testConnection();
// Parse CSV file
function parseCSV(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', reject);
    });
}

// Parse Excel file
function parseExcel(filePath) {
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
}


async function fetchReferenceData(tableName, keyColumn, valueColumn) {
    try {
        const pool = await sql.connect(config);
        // For Departments table, we want to use DepartmentName instead of Name
        const actualValueColumn = tableName === 'Departments' ? 'DepartmentName' : valueColumn;
        
        const query = `SELECT ${keyColumn}, ${actualValueColumn} FROM ${tableName}`;
        console.log('Reference data query:', query); // Debug log
        
        const result = await pool.request().query(query);
        
        // Create a mapping object { value: key }
        const mapping = result.recordset.reduce((map, row) => {
            map[row[actualValueColumn]] = row[keyColumn];
            return map;
        }, {});
        
        console.log(`Reference mapping for ${tableName}:`, mapping); // Debug log
        return mapping;
    } catch (error) {
        console.error(`Error fetching reference data for ${tableName}:`, error);
        throw error;
    }
}

async function migrateToSQL(data, tableName, mappings, customMappings = []) {
    let pool;
    let stats = {
        total: data.length,
        processed: 0,
        skipped: 0,
        failed: 0
    };

    try {
        pool = await sql.connect(config);
        
        // Get foreign key relationships
        const fkQuery = `
            SELECT 
                COL_NAME(fc.parent_object_id, fc.parent_column_id) as ColumnName,
                OBJECT_NAME(f.referenced_object_id) as ReferencedTable,
                COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as ReferencedColumn
            FROM sys.foreign_keys AS f
            INNER JOIN sys.foreign_key_columns AS fc
                ON f.object_id = fc.constraint_object_id
            WHERE OBJECT_NAME(f.parent_object_id) = @tableName
        `;

        // Get foreign key relationships
        const fkResult = await pool.request()
            .input('tableName', sql.VarChar, tableName)
            .query(`
                SELECT 
                    COL_NAME(fc.parent_object_id, fc.parent_column_id) as ColumnName,
                    OBJECT_NAME(f.referenced_object_id) as ReferencedTable,
                    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as ReferencedColumn
                FROM sys.foreign_keys AS f
                INNER JOIN sys.foreign_key_columns AS fc
                    ON f.object_id = fc.constraint_object_id
                WHERE OBJECT_NAME(f.parent_object_id) = @tableName
            `);

        // Create reference maps for all foreign keys
        const referenceMaps = {};
        for (const fk of fkResult.recordset) {
            const valueColumn = fk.ReferencedTable === 'Departments' ? 'DepartmentName' : 'Name';
            referenceMaps[fk.ColumnName] = await fetchReferenceData(
                fk.ReferencedTable,
                fk.ReferencedColumn,
                valueColumn
            );
        }

        for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
            try {
                let row = data[rowIndex];

                // Process custom mappings first
                for (const customMapping of customMappings) {
                    if (customMapping.type === 'concat') {
                        const combinedValue = customMapping.sourceFields
                            .map(field => row[field])
                            .join(customMapping.separator);
                        row[customMapping.destinationField] = combinedValue;
                    } else if (customMapping.type === 'split') {
                        const parts = row[customMapping.sourceFields[0]]?.split(customMapping.separator) || [];
                        if (Array.isArray(customMapping.destinationField)) {
                            customMapping.destinationField.forEach((field, index) => {
                                row[field] = parts[index] || '';
                            });
                        }
                    }
                }

                // Handle foreign key mappings
                for (const [columnName, referenceMap] of Object.entries(referenceMaps)) {
                    if (mappings[columnName]) {
                        const sourceValue = row[mappings[columnName]];
                        const referencedId = referenceMap[sourceValue];
                        
                        if (!referencedId) {
                            console.warn(`Reference not found for ${columnName}: ${sourceValue}`);
                            stats.failed++;
                            continue;
                        }
                        
                        row[mappings[columnName]] = referencedId;
                    }
                }

                const columns = Object.keys(mappings);
                const values = columns.map(sqlCol => row[mappings[sqlCol]]);

                // Validate data before insert
                if (values.some(v => v === undefined)) {
                    console.warn(`Warning: Row ${rowIndex + 1} has undefined values`);
                    stats.failed++;
                    continue; // Skip this row
                }

                const paramNames = columns.map((_, index) => `@p${rowIndex}_${index}`);
                const query = `
                    INSERT INTO ${tableName} (${columns.join(', ')})
                    VALUES (${paramNames.join(', ')})
                `;

                const request = pool.request();
                columns.forEach((col, index) => {
                    request.input(`p${rowIndex}_${index}`, values[index]);
                });

                await request.query(query);
                stats.processed++;

            } catch (error) {
                // Check if error is due to duplicate key
                if (error.number === 2627 || error.number === 2601) {
                    stats.skipped++;
                    console.log(`Skipped duplicate record at row ${rowIndex + 1}`);
                } else {
                    stats.failed++;
                    console.error(`Failed to process row ${rowIndex + 1}:`, error.message);
                }
            }

            // Log progress
            console.log('Current stats:', {
                total: stats.total,
                processed: stats.processed,
                skipped: stats.skipped,
                failed: stats.failed
            });
        }

        // Log final stats
        console.log('Final migration stats:', stats);
        return stats;

    } catch (error) {
        console.error('Migration Error:', error);
        throw error;
    } finally {
        if (pool) {
            await pool.close();
        }
    }
}





app.post('/api/validate', async (req, res) => {
    try {
        const { data, mappings, tableName } = req.body;
        const pool = await sql.connect(config);

        // Get reference data for foreign keys
        const fkResult = await pool.request()
            .input('tableName', sql.VarChar, tableName)
            .query(`
                SELECT 
                    COL_NAME(fc.parent_object_id, fc.parent_column_id) as ColumnName,
                    OBJECT_NAME(f.referenced_object_id) as ReferencedTable,
                    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as ReferencedColumn
                FROM sys.foreign_keys AS f
                INNER JOIN sys.foreign_key_columns AS fc
                    ON f.object_id = fc.constraint_object_id
                WHERE OBJECT_NAME(f.parent_object_id) = @tableName
            `);

        // Get reference data
        const validationResults = [];
        const referenceMaps = {};

        // Build reference maps
        for (const fk of fkResult.recordset) {
            const valueColumn = fk.ReferencedTable === 'Departments' ? 'DepartmentName' : 'Name';
            referenceMaps[fk.ColumnName] = await fetchReferenceData(
                fk.ReferencedTable,
                fk.ReferencedColumn,
                valueColumn
            );
        }

        // Validate each record
        for (const record of data) {
            const errors = [];
            
            // Check for required fields
            Object.entries(mappings).forEach(([sqlCol, fileCol]) => {
                if (!record[fileCol] || record[fileCol].trim() === '') {
                    errors.push(`Missing required value for ${sqlCol}`);
                }
            });

            // Check foreign key values
            for (const fk of fkResult.recordset) {
                const sourceValue = record[mappings[fk.ColumnName]];
                if (sourceValue && !referenceMaps[fk.ColumnName][sourceValue]) {
                    errors.push(`Invalid ${fk.ColumnName}: ${sourceValue} not found in ${fk.ReferencedTable}`);
                }
            }

            validationResults.push({
                record,
                isValid: errors.length === 0,
                errors
            });
        }

        // Calculate summary
        const summary = {
            totalRecords: data.length,
            validRecords: validationResults.filter(r => r.isValid).length,
            invalidRecords: validationResults.filter(r => !r.isValid).length,
            details: validationResults
        };

        res.json({
            success: true,
            validation: summary
        });

    } catch (error) {
        console.error('Validation Error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// app.post('/api/migrate', upload.single('file'), async (req, res) => {
//     try {
//         if (!req.file) {
//             throw new Error('No file uploaded');
//         }

//         const { tableName } = req.body;
//         const mappings = JSON.parse(req.body.mappings);
//         const customMappings = JSON.parse(req.body.customMappings || '[]');

//         if (!tableName || !mappings || Object.keys(mappings).length === 0) {
//             throw new Error('Missing required fields');
//         }

//         let data = [];
//         const fileType = req.file.originalname.split('.').pop().toLowerCase();

//         if (fileType === 'csv') {
//             data = await parseCSV(req.file.path);
//         } else if (['xlsx', 'xls'].includes(fileType)) {
//             data = parseExcel(req.file.path);
//         } else {
//             throw new Error('Unsupported file type');
//         }

//         // Migrate data and get statistics
//         const stats = await migrateToSQL(data, tableName, mappings, customMappings);

//         // Clean up uploaded file
//         fs.unlinkSync(req.file.path);

//         res.json({
//             success: true,
//             message: 'Migration completed',
//             totalRecords: stats.total,
//             processedRecords: stats.processed,
//             skippedRecords: stats.skipped,
//             failedRecords: stats.failed
//         });

//     } catch (error) {
//         console.error('API Error:', error);
//         res.status(500).json({
//             success: false,
//             error: error.message,
//             details: process.env.NODE_ENV === 'development' ? error.stack : undefined
//         });
//     }
// });

// Add endpoint to get reference data

app.post('/api/migrate', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            throw new Error('No file uploaded');
        }

        const { tableName } = req.body;
        const mappings = JSON.parse(req.body.mappings);
        const customMappings = JSON.parse(req.body.customMappings || '[]');

        if (!tableName || !mappings || Object.keys(mappings).length === 0) {
            throw new Error('Missing required fields');
        }

        let data = [];
        const fileType = req.file.originalname.split('.').pop().toLowerCase();

        if (fileType === 'csv') {
            data = await parseCSV(req.file.path);
        } else if (['xlsx', 'xls'].includes(fileType)) {
            data = parseExcel(req.file.path);
        } else {
            throw new Error('Unsupported file type');
        }

        // Initialize migration results
        const migrationResults = {
            total: data.length,
            processed: 0,
            skipped: 0,
            failed: 0,
            details: []
        };

        // Process each record
        for (let i = 0; i < data.length; i++) {
            const record = data[i];
            try {
                const pool = await sql.connect(config);
                
                // First check if record already exists
                const checkQuery = `
                    SELECT 1 FROM ${tableName} 
                    WHERE EmployeeId = @employeeId
                `;
                
                const checkResult = await pool.request()
                    .input('employeeId', sql.VarChar, record.EmployeeId)
                    .query(checkQuery);

                if (checkResult.recordset.length > 0) {
                    // Record exists - mark as skipped
                    migrationResults.skipped++;
                    migrationResults.details.push({
                        status: 'skipped',
                        record: record,
                        message: `Record with EmployeeId ${record.EmployeeId} already exists`
                    });
                    continue;
                }

                // Check if department exists
                const deptCheckQuery = `
                    SELECT 1 FROM Departments 
                    WHERE DepartmentName = @deptName
                `;
                
                const deptResult = await pool.request()
                    .input('deptName', sql.VarChar, record.DepartmentName)
                    .query(deptCheckQuery);

                if (deptResult.recordset.length === 0) {
                    // Department doesn't exist - mark as failed
                    migrationResults.failed++;
                    migrationResults.details.push({
                        status: 'failed',
                        record: record,
                        message: `Invalid department: ${record.DepartmentName}`
                    });
                    continue;
                }

                // Process and insert the record
                const processedRecord = await processRecordWithForeignKeys(record, tableName, mappings);
                const columns = Object.keys(mappings);
                const values = columns.map(col => `@${col}`);
                
                const insertQuery = `
                    INSERT INTO ${tableName} (${columns.join(', ')})
                    VALUES (${values.join(', ')})
                `;

                const request = pool.request();
                columns.forEach(col => {
                    request.input(col, processedRecord[mappings[col]]);
                });

                await request.query(insertQuery);
                
                // Record successful insertion
                migrationResults.processed++;
                migrationResults.details.push({
                    status: 'success',
                    record: record,
                    message: 'Successfully migrated'
                });

            } catch (error) {
                // Handle duplicate record error
                if (error.number === 2627 || error.number === 2601) {
                    migrationResults.skipped++;
                    migrationResults.details.push({
                        status: 'skipped',
                        record: record,
                        message: 'Duplicate record'
                    });
                } else {
                    // Handle other errors
                    migrationResults.failed++;
                    migrationResults.details.push({
                        status: 'failed',
                        record: record,
                        message: error.message
                    });
                }
            }
        }

        // Clean up uploaded file
        fs.unlinkSync(req.file.path);

        res.json({
            success: true,
            message: 'Migration completed',
            results: migrationResults
        });
        console.log(migrationResults);

    } catch (error) {
        console.error('API Error:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            details: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// Add this helper function to process individual records
async function processSingleRecord(record, tableName, mappings, customMappings) {
    const pool = await sql.connect(config);
    
    // Check for existing record
    const checkQuery = `SELECT 1 FROM ${tableName} WHERE ${Object.keys(mappings)[0]} = @id`;
    const checkResult = await pool.request()
        .input('id', sql.VarChar, record[mappings[Object.keys(mappings)[0]]])
        .query(checkQuery);

    if (checkResult.recordset.length > 0) {
        return { status: 'skipped', message: 'Record already exists' };
    }

    // Process foreign key mappings
    const processedRecord = await processRecordWithForeignKeys(record, tableName, mappings);

    // Insert the record
    const columns = Object.keys(mappings);
    const values = columns.map(col => `@${col}`);
    
    const insertQuery = `
        INSERT INTO ${tableName} (${columns.join(', ')})
        VALUES (${values.join(', ')})
    `;

    const request = pool.request();
    columns.forEach(col => {
        request.input(col, processedRecord[mappings[col]]);
    });

    await request.query(insertQuery);
    return { status: 'success' };
}

// Add this helper function to process foreign keys
async function processRecordWithForeignKeys(record, tableName, mappings) {
    const pool = await sql.connect(config);
    const processedRecord = { ...record };

    // Get foreign key information
    const fkQuery = `
        SELECT 
            COL_NAME(fc.parent_object_id, fc.parent_column_id) as ColumnName,
            OBJECT_NAME(f.referenced_object_id) as ReferencedTable,
            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as ReferencedColumn
        FROM sys.foreign_keys AS f
        INNER JOIN sys.foreign_key_columns AS fc
            ON f.object_id = fc.constraint_object_id
        WHERE OBJECT_NAME(f.parent_object_id) = @tableName
    `;

    const fkResult = await pool.request()
        .input('tableName', sql.VarChar, tableName)
        .query(fkQuery);

    // Process each foreign key
    for (const fk of fkResult.recordset) {
        const sourceValue = record[mappings[fk.ColumnName]];
        if (sourceValue) {
            const lookupQuery = `
                SELECT ${fk.ReferencedColumn}
                FROM ${fk.ReferencedTable}
                WHERE DepartmentName = @value
            `;

            const lookupResult = await pool.request()
                .input('value', sql.VarChar, sourceValue)
                .query(lookupQuery);

            if (lookupResult.recordset.length === 0) {
                throw new Error(`Referenced value not found: ${sourceValue} in ${fk.ReferencedTable}`);
            }

            processedRecord[mappings[fk.ColumnName]] = lookupResult.recordset[0][fk.ReferencedColumn];
        }
    }

    return processedRecord;
}



app.get('/api/reference/:tableName', async (req, res) => {
    try {
        const { tableName } = req.params;
        const pool = await sql.connect(config);
        
        // Get foreign key information
        const fkResult = await pool.request()
            .input('tableName', sql.VarChar, tableName)
            .query(`
                SELECT 
                    COL_NAME(fc.parent_object_id, fc.parent_column_id) as ColumnName,
                    OBJECT_NAME(f.referenced_object_id) as ReferencedTable,
                    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as ReferencedColumn
                FROM sys.foreign_keys AS f
                INNER JOIN sys.foreign_key_columns AS fc
                ON f.object_id = fc.constraint_object_id
                WHERE OBJECT_NAME(f.parent_object_id) = @tableName
            `);

        // Fetch reference data for each foreign key
        const referenceData = {};
        for (const fk of fkResult.recordset) {
            const refData = await pool.request()
                .query(`SELECT * FROM ${fk.ReferencedTable}`);
            referenceData[fk.ColumnName] = {
                table: fk.ReferencedTable,
                data: refData.recordset
            };
        }

        res.json({
            success: true,
            referenceData
        });
    } catch (error) {
        console.error('Error fetching reference data:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Add this new endpoint to get all table names
app.get('/api/tables', async (req, res) => {
    try {
        const pool = await sql.connect(config);
        const result = await pool.request().query(`
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        `);

        res.json({
            success: true,
            tables: result.recordset.map(row => row.TABLE_NAME)
        });
    } catch (error) {
        console.error('Error fetching tables:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});
// Add this new endpoint to get column names for a specific table
// app.get('/api/columns/:tableName', async (req, res) => {
//     try {
//         const pool = await sql.connect(config);
//         const result = await pool.request()
//             .input('tableName', sql.VarChar, req.params.tableName)
//             .query(`
//                 SELECT 
//                     COLUMN_NAME,
//                     DATA_TYPE,
//                     CHARACTER_MAXIMUM_LENGTH
//                 FROM INFORMATION_SCHEMA.COLUMNS
//                 WHERE TABLE_NAME = @tableName
//                 ORDER BY ORDINAL_POSITION
//             `);

//         res.json({
//             success: true,
//             columns: result.recordset.map(row => ({
//                 name: row.COLUMN_NAME,
//                 type: row.DATA_TYPE,
//                 maxLength: row.CHARACTER_MAXIMUM_LENGTH
//             }))
//         });
//     } catch (error) {
//         console.error('Error fetching columns:', error);
//         res.status(500).json({
//             success: false,
//             error: error.message
//         });
//     }
// });

// Add console.log to debug the metadata being returned
app.get('/api/columns/:tableName', async (req, res) => {
    try {
        const pool = await sql.connect(config);
        
        // Get columns first
        const columnsQuery = `
            SELECT 
                COLUMN_NAME as name,
                DATA_TYPE as type,
                CHARACTER_MAXIMUM_LENGTH as maxLength
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @tableName
            ORDER BY ORDINAL_POSITION
        `;

        const columnsResult = await pool.request()
            .input('tableName', sql.VarChar, req.params.tableName)
            .query(columnsQuery);

        // Get foreign key information
        const fkQuery = `
            SELECT 
                COL_NAME(fc.parent_object_id, fc.parent_column_id) as ColumnName,
                OBJECT_NAME(f.referenced_object_id) as ReferencedTable,
                COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as ReferencedColumn
            FROM sys.foreign_keys AS f
            INNER JOIN sys.foreign_key_columns AS fc
                ON f.object_id = fc.constraint_object_id
            WHERE OBJECT_NAME(f.parent_object_id) = @tableName
        `;

        const fkResult = await pool.request()
            .input('tableName', sql.VarChar, req.params.tableName)
            .query(fkQuery);

        // Create metadata object for ALL columns
        const metadata = {};
        
        // First, set default metadata for all columns
        columnsResult.recordset.forEach(col => {
            metadata[col.name] = {
                isForeignKey: false,
                type: col.type,
                maxLength: col.maxLength
            };
        });

        // Then update foreign key information where applicable
        fkResult.recordset.forEach(fk => {
            metadata[fk.ColumnName] = {
                ...metadata[fk.ColumnName],
                isForeignKey: true,
                referencedTable: fk.ReferencedTable,
                referencedColumn: fk.ReferencedColumn
            };
        });

        console.log('Final metadata:', metadata);

        res.json({ 
            success: true, 
            columns: columnsResult.recordset,
            metadata: metadata
        });
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});
// Add this test endpoint
app.get('/api/test-metadata/:tableName', async (req, res) => {
    try {
        const pool = await sql.connect(config);
        const fkResult = await pool.request()
            .input('tableName', sql.VarChar, req.params.tableName)
            .query(`
                SELECT 
                    COL_NAME(fc.parent_object_id, fc.parent_column_id) as ColumnName,
                    OBJECT_NAME(f.referenced_object_id) as ReferencedTable,
                    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as ReferencedColumn
                FROM sys.foreign_keys AS f
                INNER JOIN sys.foreign_key_columns AS fc
                    ON f.object_id = fc.constraint_object_id
                WHERE OBJECT_NAME(f.parent_object_id) = @tableName
            `);

        res.json({
            success: true,
            foreignKeys: fkResult.recordset
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});