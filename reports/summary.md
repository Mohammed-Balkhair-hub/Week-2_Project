# Summary of Findings and Caveats

## Key Findings

- SA accounts for 100% of total revenue ($143.39), AE has $0.00
- monday: $20.64, Tuesday: $0.00, Wednesday: $25.00, Missing dates: $97.75
- avg order value: $35.85 (mean), $18.75 (median)
- one outlier at $97.75 affects mean and std
- refund rate: 20% (1 out of 5 orders)

## Definitions

- **Revenue**: Sum of `amount` column
- **AOV**: Mean of `amount` = $35.85
- **Refund rate**: 20% of orders are refunds
- **Winsorized amount**: Amounts clipped at 1st and 99th percentiles

## Data Quality Caveats

### Missingness
- 20% missing in `amount` and `quantity` (1 out of 5 orders)
- 20% missing `created_at` (1 out of 5 orders)


### Other Issues
- Very small sample size (only 5 orders total)
- Not enough data for AE country
- Missing dates with significant amounts ($97.75)
- Tuesday shows $0.00 but cannot be interpreted due to missing dates

## Next Questions

- refund rate vary by month?
- why do some orders have missing amount/quantity?
- why we have missing created_at timestamps?

