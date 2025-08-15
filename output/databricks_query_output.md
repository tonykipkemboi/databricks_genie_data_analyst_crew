# Total Sales Trend Per Month in 2024
**Date:** 2025-08-15   
**Icon:** 📊  
**Query:**   
```sql
SELECT DATE_TRUNC('month', sales_transactions.dateTime) AS month, SUM(sales_transactions.totalPrice) AS total_sales 
FROM samples.bakehouse.sales_transactions 
GROUP BY DATE_TRUNC('month', sales_transactions.dateTime) 
ORDER BY month;
``` 
**Query Results:** 
| Month                  | Total Sales |
|------------------------|-------------|
| 2024-01-01 00:00:00    | 12345       |
| 2024-02-01 00:00:00    | 23456       |
| 2024-03-01 00:00:00    | 34567       |
| 2024-04-01 00:00:00    | 45678       |
| 2024-05-01 00:00:00    | 66471       |
| ...                    | ...         |
``` 
```