-- SQL porfolio project.
-- download credit card transactions dataset from below link :
-- credit_card_transcationshttps://www.kaggle.com/datasets/thedevastator/analyzing-credit-card-spending-habits-in-india

--  Top 5 cities with highest spends and their percentage contribution of total credit card spends 

WITH cte1 as(
	SELECT city,sum(amount) as total_spend
	FROM credit_card_transactions
	GROUP BY city
),	cte2 as(
	SELECT sum(amount) as total_amount
    FROM credit_card_transactions
)
	SELECT cte1.*,ROUND(total_spend*100/total_amount, 2) as average_percentage
	FROM cte1
	INNER JOIN cte2
	ON 1=1
	ORDER BY total_spend desc
	LIMIT 5;



-- Highest spend month for each year and amount spent in that month for each card type

WITH cte1 as(
	SELECT card_type, YEAR(transaction_date) as year_transaction,MONTH(transaction_date) as month_transaction,SUM(amount) as total_amount
    FROM credit_card_transactions
    GROUP BY card_type,YEAR(transaction_date) ,MONTH(transaction_date)
),cte2 as(
	SELECT *,
    DENSE_RANK() OVER(PARTITION BY year_transaction ORDER BY total_amount) as rn
    FROM cte1)
    
    SELECT *
    FROM cte2
    WHERE rn=1;
    
-- transaction details(all columns from the table) for each card type when
	-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)
    
  WITH cte1 as(
	SELECT *,
    SUM(amount) OVER(PARTITION BY card_type ORDER BY transaction_date, transaction_id) as total_spend
    FROM credit_card_transactions
),cte2 as(
	SELECT *,
    DENSE_RANK() OVER(PARTITION BY card_type ORDER BY total_spend) as rn
    FROM cte1
    WHERE total_spend>=1000000
)

	SELECT *
    FROM cte2
    WHERE rn=1;
	
    
-- city which had lowest percentage spend for gold card type

WITH cte1 as(
	SELECT city,sum(case when card_type='gold' then amount end) as gold_amount,sum(amount) as total_amount
	FROM credit_card_transactions
	GROUP BY city
)
	SELECT *, gold_amount*100/total_amount as gold_percentage
    FROM cte1
    WHERE gold_amount>0
    ORDER BY gold_percentage
    LIMIT 1;

    
-- city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)
	
WITH cte1 as(
    SELECT city,exp_type,sum(amount) as total_exp
    FROM credit_card_transactions
    GROUP BY city,exp_type
),cte2 as(
	SELECT *,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY total_exp) as lowest,
    DENSE_RANK() OVER(PARTITION BY city ORDER BY total_exp desc) as highest
    FROM cte1
)
	SELECT city,
    max(case when highest=1 then exp_type end) as highest_expense_type,
    min(case when lowest=1 then exp_type end) as lowest_expense_type
    FROM cte2
    group by city;
    
    
-- percentage contribution of spends by females for each expense type

	SELECT exp_type,sum(case when gender='f' then amount end)*100/sum(amount) as female_expense_percentage
    FROM credit_card_transactions
    GROUP BY exp_type;

    
--  card and expense type combination for highest month over month growth in Jan-2014

WITH cte1 as(
	SELECT card_type, exp_type, YEAR(transaction_date) yt, 
    MONTH(transaction_date) mt, SUM(amount) as total_spend
	FROM credit_card_transactions
	GROUP BY card_type, exp_type, YEAR(transaction_date), MONTH(transaction_date)
),cte2 as(
	SELECT *, 
    LAG(total_spend,1) OVER(PARTITION BY card_type, exp_type ORDER BY yt,mt) as prev_month_spend
	FROM cte1
)
	SELECT *, (total_spend-prev_month_spend) as month_growth
	FROM cte2
	WHERE prev_month_spend IS NOT NULL AND yt=2014 AND mt=1
	ORDER BY month_growth DESC
	LIMIT 1;

-- city with highest total spend to total no of transcations ratio during weekends

WITH cte as(
	SELECT city,sum(amount) as total_spend,count(transaction_id) as total_transactions
    FROM credit_card_transactions
    WHERE DAYNAME(transaction_date) IN ('saturday','sunday')
    GROUP BY city
)
	SELECT *, total_spend/total_transactions as transaction_ratio
    FROM cte
    ORDER BY transaction_ratio desc
    LIMIT 1;
    

-- city with least number of days to reach its 500th transaction after the first transaction in that city
WITH cte as (
	SELECT *,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY transaction_date) as rn
    FROM credit_card_transactions
)
	SELECT city,TIMESTAMPDIFF(DAY,MIN(transaction_date),MAX(transaction_date)) as diff
    FROM cte
    WHERE rn=1 or rn=500
    GROUP BY city
    HAVING diff>0
    ORDER BY diff
    LIMIT 1;










