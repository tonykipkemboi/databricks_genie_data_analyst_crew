# Which Product Sold the Most Units?
**Date:** 2025-08-15  **Icon:** 📊  
**Query:**  
```sql
SELECT `product`, SUM(`quantity`) AS `total_units_sold`, SUM(`totalPrice`) AS `total_revenue` 
FROM `samples`.`bakehouse`.`sales_transactions` 
WHERE `product` IS NOT NULL 
GROUP BY `product` 
ORDER BY `total_units_sold` DESC 
LIMIT 1;
```
**Query Results:**  
| Product              | Total Units Sold | Total Revenue |
|---------------------|------------------|----------------|
| Golden Gate Ginger   | 3865             | 11595          | 