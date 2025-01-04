PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE all_active_stocks (
                symbol TEXT PRIMARY KEY,
                assetType TEXT 
            );
COMMIT;
