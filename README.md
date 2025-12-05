# exact_avg – High-Precision AVG() UDX for Vertica

---

## ⚠️ Disclaimer

This software - including the `exact_avg` UDx implementation, supporting scripts, and documentation - is provided **“as is”**, without any warranties of any kind, whether express or implied.  
No guarantees are made regarding accuracy, reliability, performance, or suitability for any particular purpose.  

Before using this code in production systems or mission - critical environments, you should:

- Validate correctness with your own datasets,  
- Perform extensive testing under expected workload conditions, and  
- Review Vertica’s documentation regarding UDx development and NUMERIC precision handling.

By using this software, you agree that you assume full responsibility for any outcomes.

---


## 1. Overview

This project provides a Vertica User Defined Aggregate Function (UDAF) called `exact_avg`
for computing averages on very large `NUMERIC(p, s)` values with:

- **High precision** for extreme numeric ranges and very large row counts.
- **Dynamic precision management** for performance.
- **Clear diagnostics** when the required precision exceeds Vertica’s `NUMERIC(1024, s)` limit.

Vertica’s built-in aggregates work well for typical workloads.  
This UDX enhances precision handling for rare **extreme-value** scenarios where users require:

- additional numeric safety,
- guaranteed accuracy when possible,
- explicit errors instead of silent truncation.

`exact_avg` will either return the mathematically correct average or clearly explain why the calculation cannot be guaranteed within Vertica’s numeric limits.

---

## 2. What `exact_avg` does

### Function Signature

```sql
exact_avg(a NUMERIC(p, s)) RETURNS NUMERIC(p_out, s_out)
```

Where:

```
p_out = min(1024, p_in + 5)
s_out = min(p_out, s_in + 5)
```

This ensures the output:

- Preserves the input scale.
- Adds a few digits for precision.
- Adapts dynamically to the input column’s numeric properties.

---

## 3. Internal Approach 

### Intermediate state contains:

- A **wide NUMERIC sum** type (`p_sum = min(1024, p_in + 19)`),
- Row count (`cnt`),
- Input precision/scale (`p_in`, `s_in`).

The UDX adds **19 digits** to internal precision because:

- `ceil(log10(N))` for any Vertica row count (`N ≤ 9e18`) is ≤ 19.

This makes the SUM precise **whenever it is mathematically representable** within Vertica’s maximum precision.

### Final Step Logic

During termination:

1. Compute `digits(rowCount)`
2. Compute required precision:

   ```
   p_needed = p_in + digits(rowCount)
   ```

3. If `p_needed > 1024`  
   → No exact SUM is possible within Vertica's numeric limits.  
   → UDX returns a **clear diagnostic error**.

4. Otherwise  
   → SUM fits exactly, division is exact, the result is mathematically correct.

---

## 4. Repository Contents

| File | Description |
|------|-------------|
| **exact_avg.cpp** | UDX implementation using Vertica SDK |
| **Makefile** | Builds `/tmp/exact_avg.so` |
| **1_compile.sh** | Wrapper script invoking `make` |
| **2_register_and_test.sql** | Registers UDX + small sample test |
| **3_stress_test.sql** | Extreme dataset test (up to 100M rows or nore) |

---

## 5. Build Instructions

Run on the Vertica node with SDK installed:

```bash
./1_compile.sh
```

Or manually:

```bash
make
```

The Makefile prints whether build succeeded or failed.

---

## 6. Register the UDX

```bash
vsql -ef 2_register_and_test.sql
```

This:

1. Creates `exact_avg_lib`
2. Creates `exact_avg` aggregate
3. Grants PUBLIC access
4. Runs a 5-row numeric accuracy test

---

## 7. Stress Test

To test extreme numeric conditions:

```bash
vsql -ef 3_stress_test.sql
```

This script:

- Creates 100 million rows of very large NUMERIC values,
- Compares:
  - `SUM(a)/COUNT(a)`
  - `AVG(a)`
  - `exact_avg(a)`
- Computes the true mathematical average analytically:

  ```
  BASE + (n + 1)/2
  ```

- Shows that:

  ```
  exact_avg(a) - expected_avg = 0.00000
  ```

---

## 8. Using exact_avg in Your Own Queries

```sql
SELECT exact_avg(a) FROM big_table;

SELECT customer_id, exact_avg(order_total)
FROM orders
GROUP BY customer_id;
```

- NULLs are ignored (standard SQL behavior).
- Returns NULL if all rows in a group are NULL.
- Provides precise results or clear diagnostics when precision is mathematically impossible.

---

## 9. Notes

- This UDX respects Vertica's global numeric limit (`NUMERIC(1024, s)`).
- It is designed for **extreme** numeric workloads, not typical queries.
- Produces either:
  - exact mathematical result, or
  - explicit explanation of why precision cannot be guaranteed.

---

## 10. Summary

`exact_avg` offers:

- Dynamic precision handling  
- Mathematical guarantees when possible  
- Transparent diagnostics when limits are exceeded  
- Performance suitable for large datasets  
- Drop-in replacement for special high-precision needs  

It is a helpful option for applications requiring **maximum numeric precision** in Vertica.

