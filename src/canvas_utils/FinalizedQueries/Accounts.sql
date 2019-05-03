DROP TABLE IF EXISTS Accounts;
CREATE TABLE Accounts (
    account_id bigint,
    account_name varchar(255)
    ) engine=MyISAM;
    
# <end_creation>


INSERT INTO Accounts
SELECT id, name
  FROM canvasdata_prd.account_dim;