/**
 * mappers/handlers/route-handlers.js -- Route processor smart code generation
 *
 * Extracted from index.html lines ~4080-4124, 4713-4777, 5342-5349.
 * Handles: RouteOnAttribute, RouteOnContent, RouteText, ValidateRecord,
 * DistributeLoad, DetectDuplicate.
 * GAP FIX: Added MIME-type content routing via regexp_extract on content headers.
 */

/**
 * Generate Databricks code for route-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @param {function} translateNELtoPySpark - NEL translation function
 * @returns {{ code: string, conf: number }|null}
 */
export function handleRouteProcessor(p, props, varName, inputVar, existingCode, existingConf, translateNELtoPySpark) {
  let code = existingCode;
  let conf = existingConf;

  // -- RouteOnAttribute --
  if (p.type === 'RouteOnAttribute') {
    const strategy = props['Routing Strategy'] || 'Route to Property name';
    const routeEntries = Object.entries(props).filter(([k]) => k !== 'Routing Strategy');
    if (routeEntries.length) {
      const lines = [
        'from pyspark.sql.functions import col, lit, upper, lower, trim, length, substring, regexp_replace, concat, when, current_timestamp, date_format, to_timestamp, expr, rand, substring_index, lpad, rpad, locate, split, regexp_extract, round, abs, ceil, floor',
        `# RouteOnAttribute: ${p.name} — ${strategy}`,
        `# Generates named DataFrames per route with NEL-parsed filter conditions`
      ];
      const routeVars = [];
      routeEntries.forEach(([routeName, rawExpr]) => {
        const safe = routeName.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
        const routeVar = `df_${varName}_${safe}`;
        routeVars.push(routeVar);
        if (rawExpr.includes('${')) {
          const filterExpr = translateNELtoPySpark(rawExpr, 'col');
          lines.push(`# Route "${routeName}": ${rawExpr.substring(0,80).replace(/"/g,"'")}`);
          lines.push(`${routeVar} = df_${inputVar}.filter(${filterExpr})`);
        } else {
          lines.push(`# Route "${routeName}": ${rawExpr}`);
          if (/^true$/i.test(rawExpr)) {
            lines.push(`${routeVar} = df_${inputVar}  # Always matches`);
          } else if (/^false$/i.test(rawExpr)) {
            lines.push(`${routeVar} = df_${inputVar}.limit(0)  # Never matches`);
          } else {
            lines.push(`${routeVar} = df_${inputVar}.filter(lit(True))  # Static: ${rawExpr.substring(0,60)}`);
          }
        }
      });
      // Compute unmatched as subtract of all matched routes
      if (routeVars.length === 1) {
        lines.push(`df_${varName}_unmatched = df_${inputVar}.subtract(${routeVars[0]})`);
      } else if (routeVars.length > 1) {
        lines.push(`# Unmatched: rows not matching any route`);
        lines.push(`df_${varName}_unmatched = df_${inputVar}`);
        routeVars.forEach(rv => {
          lines.push(`df_${varName}_unmatched = df_${varName}_unmatched.subtract(${rv})`);
        });
      }
      lines.push(`df_${varName} = df_${inputVar}  # Pass-through for default routing`);
      code = lines.join('\n');
      conf = 0.92;
      return { code, conf };
    }
  }

  // -- RouteOnContent (GAP FIX: MIME-type content routing) --
  if (p.type === 'RouteOnContent') {
    const contentReq = props['Content Requirement'] || '.*';
    const matchReq = props['Match Requirement'] || 'content must contain match';
    // Check for MIME-type routing patterns
    const routeEntries = Object.entries(props).filter(([k]) =>
      !['Content Requirement', 'Match Requirement', 'Character Set', 'Buffer Size'].includes(k));

    if (routeEntries.length > 0) {
      const lines = [
        'from pyspark.sql.functions import col, regexp_extract, when, lit',
        `# RouteOnContent: ${p.name}`,
        `# Match: ${matchReq}`
      ];
      const routeVars = [];
      routeEntries.forEach(([routeName, pattern]) => {
        const safe = routeName.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
        const rv = `df_${varName}_${safe}`;
        routeVars.push(rv);
        lines.push(`# Route "${routeName}" — pattern: ${pattern.substring(0,60)}`);
        lines.push(`${rv} = df_${inputVar}.filter(col("value").rlike("${pattern.replace(/"/g, '\\"')}"))`);
      });
      // MIME-type routing via content header extraction
      lines.push(`# GAP FIX: MIME-type routing — extract Content-Type from headers if present`);
      lines.push(`if "content_type" in df_${inputVar}.columns:`);
      lines.push(`    df_${varName}_json = df_${inputVar}.filter(col("content_type").rlike("application/json"))`);
      lines.push(`    df_${varName}_xml = df_${inputVar}.filter(col("content_type").rlike("(application|text)/xml"))`);
      lines.push(`    df_${varName}_csv = df_${inputVar}.filter(col("content_type").rlike("text/csv"))`);
      // Unmatched: subtract all matched route DataFrames from input
      lines.push(`# Unmatched: rows not matching any content route`);
      lines.push(`df_${varName}_unmatched = df_${inputVar}`);
      routeVars.forEach(rv => {
        lines.push(`df_${varName}_unmatched = df_${varName}_unmatched.subtract(${rv})`);
      });
      lines.push(`df_${varName} = df_${inputVar}  # Pass-through for default routing`);
      code = lines.join('\n');
    } else {
      code = `# RouteOnContent: ${p.name}\nfrom pyspark.sql.functions import col\n# Route based on content matching\ndf_${varName}_matched = df_${inputVar}.filter(col("value").rlike("${contentReq}"))\ndf_${varName}_unmatched = df_${inputVar}.subtract(df_${varName}_matched)\ndf_${varName} = df_${inputVar}`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- RouteText --
  if (p.type === 'RouteText') {
    code = `# RouteText: ${p.name}\nfrom pyspark.sql.functions import col\ndf_${varName} = df_${inputVar}\n# Route text lines by pattern matching\nprint(f"[ROUTE] Text routing applied")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- ValidateRecord --
  if (p.type === 'ValidateRecord') {
    const schemaName = props['Schema Name'] || '';
    const schemaText = props['Schema Text'] || '';
    const strategy = props['Schema Access Strategy'] || 'Inherit Record Schema';
    const invalidAction = props['Invalid Record Strategy'] || 'route';
    const validationRules = Object.entries(props).filter(([k]) =>
      !['Schema Name','Schema Text','Schema Access Strategy','Record Reader','Record Writer','Invalid Record Strategy','Allow Extra Fields','Strict Type Checking'].includes(k));

    const lines = [
      'from pyspark.sql.functions import col, lit, when, current_timestamp',
      `# ValidateRecord: ${p.name}`,
      `# Schema: ${schemaName || 'inferred'} | Strategy: ${strategy} | On Invalid: ${invalidAction}`
    ];

    // DLT expectations
    lines.push(`# DLT Expectations (use when running as Delta Live Table):`);
    if (validationRules.length > 0) {
      validationRules.forEach(([ruleName, ruleExpr]) => {
        const safeRule = ruleName.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
        if (ruleExpr.includes('${')) {
          const parsedExpr = translateNELtoPySpark(ruleExpr, 'col');
          lines.push(`# @dlt.expect_or_drop("${safeRule}", "${parsedExpr.replace(/"/g, "'").substring(0,100)}")`);
        } else {
          lines.push(`# @dlt.expect_or_drop("${safeRule}", "${ruleExpr.replace(/"/g, "'").substring(0,100)}")`);
        }
      });
    } else {
      lines.push(`# @dlt.expect_or_drop("not_null_check", "col('${schemaName || 'id'}') IS NOT NULL")`);
    }

    // Inline validation
    lines.push('');
    lines.push('# Inline validation — split valid/invalid records');
    if (validationRules.length > 0) {
      const conditions = validationRules.map(([rn, re]) => {
        if (re.includes('${')) return translateNELtoPySpark(re, 'col');
        return `col("${rn}").isNotNull()`;
      });
      const combinedCond = conditions.join(' & ');
      lines.push(`_valid_cond = ${combinedCond}`);
      lines.push(`df_${varName}_valid = df_${inputVar}.filter(_valid_cond)`);
      lines.push(`df_${varName}_invalid = df_${inputVar}.filter(~(_valid_cond))`);
    } else {
      const checkCol = schemaName || 'id';
      lines.push(`df_${varName}_valid = df_${inputVar}.filter(col("${checkCol}").isNotNull())`);
      lines.push(`df_${varName}_invalid = df_${inputVar}.filter(col("${checkCol}").isNull())`);
    }
    lines.push(`df_${varName} = df_${varName}_valid`);
    lines.push('');
    lines.push(`# Dead-letter queue for invalid records`);
    lines.push(`_valid_count = df_${varName}_valid.count()`);
    lines.push(`_invalid_count = df_${varName}_invalid.count()`);
    lines.push(`if _invalid_count > 0:`);
    lines.push(`    df_${varName}_invalid.withColumn("_rejected_at", current_timestamp()).withColumn("_rejection_source", lit("${p.name.replace(/"/g,'\\"')}")).write.mode("append").saveAsTable("<catalog>.<schema>.__dead_letter_queue")`);
    lines.push(`print(f"[VALIDATE] {_valid_count} valid, {_invalid_count} invalid records")`);
    code = lines.join('\n');
    conf = 0.92;
    return { code, conf };
  }

  // -- DistributeLoad --
  if (p.type === 'DistributeLoad') {
    const numRels = props['Number of Relationships'] || '4';
    code = `# Distribute Load: ${p.name}\ndf_${varName} = df_${inputVar}.repartition(${numRels})\nprint(f"[DISTRIBUTE] Repartitioned to ${numRels} partitions")`;
    conf = 0.93;
    return { code, conf };
  }

  // -- DetectDuplicate --
  if (p.type === 'DetectDuplicate') {
    code = `# Dedup: ${p.name}\ndf_${varName} = df_${inputVar}.dropDuplicates()\nprint(f"[DEDUP] Removed duplicates")`;
    conf = 0.93;
    return { code, conf };
  }

  return null; // Not handled
}
