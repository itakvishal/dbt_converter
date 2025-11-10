{{
    config(
        materialized = 'incremental',
        unique_key = 'tidewallet_transaction_id',
        tags = ['1hour','daily'],
        on_schema_change='sync_all_columns'
    )
}}

WITH base_transactions AS (
  SELECT
       tidewallet_transaction_id
      ,issuer
      ,updated_by_id
      ,updated_by_type
      ,tt.account_id
      ,booking_date_time
      ,creditor_account_identification
      ,creditor_name
      ,debtor_account_identification
      ,debtor_name
      ,local_instrument
      ,mcc
      ,tt.transaction_reference
      ,value_date_time
      ,balance_transaction_id
      ,reference_transaction_id
      ,amount_currency
      ,amount_amount
      ,amount_lc_currency
      ,amount_lc_amount
      ,creation_datetime
      ,`type`
      ,status_reason
      ,card_auth_type
      ,other_account_id
      ,other_account_type
      ,wallet_account_id
      ,other_wallet_account_id
      ,exchange_id
      ,debtor_account_id
      ,creditor_account_id
      ,external_transaction_tracking_id
      ,tt.transaction_id
      ,updation_datetime
      ,transaction_link_id
      ,credit_debit_indicator
      ,transaction_information
      ,status
      ,migration_date_time
      ,additional_data
      ,card_identification
      ,card_serial_no
      ,creditor_agent_identification
      ,creditor_agent_scheme_name
      ,creditor_scheme_name
      ,debtor_agent_identification
      ,debtor_scheme_name
      ,fee_amount_amount
      ,fee_amount_currency
      ,instrument_sub_type
      ,merchant_name
      ,modification_completed_on
      ,original_transaction_reference
      ,txn_alert_seq_number
      ,card_int_no
      ,event_json
      ,tt.transaction_loaded_at
      ,balance
      ,balance_changed_on
      ,card_id
      ,tt.company_id
      ,debtor_agent_scheme_name
      ,issuer_institution
      ,merchant_location
      ,merchant_identifier
      ,reservation_balance
      ,a.tis_transaction_type
      ,la.account_number
      ,la.sort_code
      ,la.external_account_number
      ,approval_info
      ,initiation_info
      ,tide_fee_amount_amount
      ,tide_fee_amount_currency
      ,tide_fee_external_tracking_id
  FROM {{ ref ('latest_tidewallet_transactions') }} tt
  LEFT JOIN {{ ref('chnl_tis_transactions') }} a
    ON tt.transaction_id = a.transaction_id
  LEFT JOIN {{ ref('latest_accounts')}} la
    ON tt.account_id = la.account_id
  {% if is_incremental() %}
     WHERE tt.transaction_loaded_at > (SELECT MAX(transaction_loaded_at) FROM {{ this }})
  {% endif %}
),

interim_clear_balances AS (
    SELECT
        tidewallet_transaction_id,
        JSON_QUERY_ARRAY(balance, '$.balances') AS balances_array,
        `type`,
        credit_debit_indicator,
        creditor_name,
        transaction_information,
        debtor_name,
        local_instrument,
        merchant_name
    FROM base_transactions
),

unnested_interim_clear_balances AS (
    SELECT
        tidewallet_transaction_id,
        elem,
        `type`,
        credit_debit_indicator,
        creditor_name,
        transaction_information,
        debtor_name,
        local_instrument,
        merchant_name
    FROM interim_clear_balances
    CROSS JOIN UNNEST(JSON_QUERY_ARRAY(balances_array)) AS elem
    WHERE JSON_VALUE(elem, '$.type') = 'InterimCleared'
),

final_balances AS (
    SELECT
        tidewallet_transaction_id,
        `type`,
        credit_debit_indicator,
        creditor_name,
        transaction_information,
        debtor_name,
        local_instrument,
        merchant_name,
        SAFE_CAST(JSON_VALUE(elem, '$.amount.amount') AS NUMERIC) / 100 AS interim_cleared_amount,
        IF(JSON_VALUE(elem, '$.creditDebitIndicator') = 'DEBIT', -1 * (SAFE_CAST(JSON_VALUE(elem, '$.amount.amount') AS NUMERIC) / 100), (SAFE_CAST(JSON_VALUE(elem, '$.amount.amount') AS NUMERIC) / 100)) AS new_balance_amount
    FROM unnested_interim_clear_balances
),

final AS (
  SELECT
       bt.tidewallet_transaction_id
      ,bt.issuer
      ,bt.updated_by_id
      ,bt.updated_by_type
      ,bt.account_id
      ,bt.booking_date_time
      ,bt.creditor_account_identification
      ,bt.creditor_name
      ,bt.debtor_account_identification
      ,bt.debtor_name
      ,bt.local_instrument
      ,bt.mcc
      ,bt.transaction_reference
      ,bt.value_date_time
      ,bt.balance_transaction_id
      ,bt.reference_transaction_id
      ,bt.amount_currency
      ,bt.amount_amount
      ,bt.amount_lc_currency
      ,bt.amount_lc_amount
      ,bt.creation_datetime
      ,bt.`type`
      ,bt.status_reason
      ,bt.card_auth_type
      ,bt.other_account_id
      ,bt.other_account_type
      ,bt.wallet_account_id
      ,bt.other_wallet_account_id
      ,bt.exchange_id
      ,bt.debtor_account_id
      ,bt.creditor_account_id
      ,bt.external_transaction_tracking_id
      ,bt.transaction_id
      ,bt.updation_datetime
      ,bt.transaction_link_id
      ,bt.credit_debit_indicator
      ,bt.transaction_information
      ,CASE
          -- DEBIT
          WHEN (bt.`type` IN ('DOMESTIC_TRANSFER', 'OWN_ACCOUNT_TRANSFER', 'DIRECT_DEBIT', 'CROSSBORDER_TRANSFER')) AND (bt.credit_debit_indicator = 'DEBIT')
              AND (bt.creditor_name IS NOT NULL) AND (bt.transaction_information IS NOT NULL) THEN  CONCAT(bt.creditor_name, ' ref: ', bt.transaction_information)
          WHEN (bt.`type` IN ('DOMESTIC_TRANSFER', 'OWN_ACCOUNT_TRANSFER', 'DIRECT_DEBIT', 'CROSSBORDER_TRANSFER')) AND (bt.credit_debit_indicator = 'DEBIT')
              AND (bt.creditor_name IS NOT NULL) AND (bt.transaction_information IS NULL) THEN  bt.creditor_name
          WHEN (bt.`type` IN ('DOMESTIC_TRANSFER', 'OWN_ACCOUNT_TRANSFER', 'DIRECT_DEBIT', 'CROSSBORDER_TRANSFER')) AND (bt.credit_debit_indicator = 'DEBIT')
              AND (bt.creditor_name IS NULL) AND (bt.transaction_information IS NOT NULL) THEN  CONCAT('ref:', bt.transaction_information)

          -- CREDIT
          WHEN (bt.`type` IN ('DOMESTIC_TRANSFER', 'DOMESTIC_TRANSFER_REFUND', 'OWN_ACCOUNT_TRANSFER', 'INTERNAL_BOOK_TRANSFER', 'DIRECT_DEBIT', 'CROSSBORDER_TRANSFER','REWARD')) AND (bt.credit_debit_indicator = 'CREDIT')
              AND (bt.debtor_name IS NOT NULL) AND (bt.transaction_information IS NOT NULL) THEN CONCAT(bt.debtor_name, ' ref: ', bt.transaction_information)
          WHEN (bt.`type` IN ('DOMESTIC_TRANSFER', 'DOMESTIC_TRANSFER_REFUND', 'OWN_ACCOUNT_TRANSFER', 'INTERNAL_BOOK_TRANSFER', 'DIRECT_DEBIT', 'CROSSBORDER_TRANSFER','REWARD')) AND (bt.credit_debit_indicator = 'CREDIT')
              AND (bt.debtor_name IS NOT NULL) AND (bt.transaction_information IS NULL) THEN bt.debtor_name
          WHEN (bt.`type` IN ('DOMESTIC_TRANSFER', 'DOMESTIC_TRANSFER_REFUND', 'OWN_ACCOUNT_TRANSFER', 'INTERNAL_BOOK_TRANSFER', 'DIRECT_DEBIT', 'CROSSBORDER_TRANSFER','REWARD','ACQUIRER_SETTLEMENT')) AND (bt.credit_debit_indicator = 'CREDIT')
              AND (bt.debtor_name IS NULL) AND (bt.transaction_information IS NOT NULL) THEN CONCAT('ref:', bt.transaction_information)

          -- DOMESTIC_TRANSFER_REFUND
          WHEN (bt.`type` = 'DOMESTIC_TRANSFER_REFUND') THEN 'Faster payment refund'

          -- INTERNAL_BOOK_TRANSFER (DEBIT)
          WHEN (bt.`type` = 'INTERNAL_BOOK_TRANSFER') AND (bt.credit_debit_indicator = 'DEBIT') AND (bt.transaction_information LIKE '%tide fees%') THEN bt.transaction_information
          WHEN (bt.`type` = 'INTERNAL_BOOK_TRANSFER') AND (bt.credit_debit_indicator = 'DEBIT') AND (bt.creditor_name IS NOT NULL) AND (bt.transaction_information IS NOT NULL)
              AND (bt.transaction_information not LIKE '%tide fees%') THEN  CONCAT(bt.creditor_name, ' ref: ', bt.transaction_information)
          WHEN (bt.`type` = 'INTERNAL_BOOK_TRANSFER') AND (bt.credit_debit_indicator = 'DEBIT') AND (bt.creditor_name IS NOT NULL) AND (bt.transaction_information IS NULL)
              AND (bt.transaction_information not LIKE '%tide fees%') THEN  bt.creditor_name
          WHEN (bt.`type` = 'INTERNAL_BOOK_TRANSFER') AND (bt.credit_debit_indicator = 'DEBIT') AND (bt.creditor_name IS NULL) AND (bt.transaction_information IS NOT NULL)
              AND (bt.transaction_information not LIKE '%tide fees%') THEN  CONCAT('ref:', bt.transaction_information)

          -- FEE
          WHEN (bt.`type` = 'FEE') THEN 'Fees: Bank transfer AND Direct Debit for last month (20p per transaction)'

          -- ADJUSTMENT
          WHEN (bt.`type` = 'ADJUSTMENT') AND (bt.credit_debit_indicator IN ('DEBIT')) AND (bt.transaction_information = 'MIGRATION_CHAPS_HELD_ADJUSTMENT') THEN 'CHAPS payment held'
          WHEN (bt.`type` = 'ADJUSTMENT') AND (bt.credit_debit_indicator IN ('DEBIT')) AND (bt.transaction_information != 'MIGRATION_CHAPS_HELD_ADJUSTMENT') THEN 'Tide debit'
          WHEN (bt.`type` = 'ADJUSTMENT') AND (bt.credit_debit_indicator IN ('CREDIT')) AND (bt.transaction_information = 'MIGRATION_CHAPS_HELD_ADJUSTMENT') THEN 'CHAPS payment released'
          WHEN (bt.`type` = 'ADJUSTMENT') AND (bt.credit_debit_indicator IN ('CREDIT')) AND (bt.transaction_information != 'MIGRATION_CHAPS_HELD_ADJUSTMENT') THEN 'Tide credit'

          --CASH_WITHDRAWAL_REVERSAL
          WHEN (bt.`type` IN('CASH_WITHDRAWAL_REVERSAL','CARD_TRANSACTION_REVERSAL','CARD_TRANSACTION_REFUND') AND (bt.merchant_name IS NOT NULL) AND (bt.transaction_information IS NOT NULL)) THEN CONCAT(bt.merchant_name, ' ref: ', bt.transaction_information)
          WHEN (bt.`type` IN('CASH_WITHDRAWAL_REVERSAL','CARD_TRANSACTION_REVERSAL','CARD_TRANSACTION_REFUND') AND (bt.merchant_name IS NOT NULL) AND (bt.transaction_information IS NULL)) THEN bt.merchant_name
          WHEN (bt.`type` IN('CASH_WITHDRAWAL_REVERSAL','CARD_TRANSACTION_REVERSAL','CARD_TRANSACTION_REFUND') AND (bt.merchant_name IS NULL) AND (bt.transaction_information IS NOT NULL)) THEN CONCAT(' ref: ', bt.transaction_information)

          --CARD_TRANSACTION_CHARGE
          WHEN (bt.`type` IN('CARD_TRANSACTION_CHARGE') AND (bt.credit_debit_indicator = 'DEBIT') AND (bt.merchant_name IS NOT NULL) AND (bt.transaction_information IS NOT NULL)) THEN CONCAT(bt.merchant_name, ' ref: ', bt.transaction_information)
          WHEN (bt.`type` IN('CARD_TRANSACTION_CHARGE') AND (bt.credit_debit_indicator = 'DEBIT') AND (bt.merchant_name IS NOT NULL) AND (bt.transaction_information IS NULL)) THEN bt.merchant_name
          WHEN (bt.`type` IN('CARD_TRANSACTION_CHARGE') AND (bt.credit_debit_indicator = 'DEBIT') AND (bt.merchant_name IS NULL) AND (bt.transaction_information IS NOT NULL)) THEN CONCAT(' ref: ', bt.transaction_information)

          --FX_TRANSFER --No matching fields
          --  WHEN (bt.`type` = 'FX_TRANSFER') AND (bt.credit_debit_indicator IN ('CREDIT')) AND (fx_source_currency IS NOT NULL) THEN CONCAT('FX conversion from',fx_source_currency,'account')
          --  WHEN (bt.`type` = 'FX_TRANSFER') AND (bt.credit_debit_indicator IN ('CREDIT')) AND (fx_source_currency IS NULL) THEN fx_source_currency
          --  WHEN (bt.`type` = 'FX_TRANSFER') AND (bt.credit_debit_indicator IN ('DEBIT')) AND (fx_target_currency IS NOT NULL) THEN CONCAT('FX conversion to',fx_target_currency,'account')
          --  WHEN (bt.`type` = 'FX_TRANSFER') AND (bt.credit_debit_indicator IN ('DEBIT')) AND (fx_target_currency IS  NULL) THEN fx_target_currency

          --CASH_DEPOSIT
           WHEN (bt.`type` = 'CASH_DEPOSIT') AND (bt.credit_debit_indicator IN ('CREDIT')) THEN INITCAP(REPLACE(bt.local_instrument,'_',' '))

          --OTHERS
          WHEN (bt.`type` IN ('INTEREST','INTEREST_REVERSAL')) THEN bt.transaction_information
          WHEN (bt.`type` IN ('DOMESTIC_TRANSFER_REVERSAL','DIRECT_DEBIT_REFUND','DIRECT_DEBIT_REFUND_REVERSAL','CARD_TO_CARD_TRANSFER','CARD_TRANSACTION','CARD_TRANSACTION_REFUND_REVERSAL','CASH_DEPOSIT_REVERSAL','CASH_WITHDRAWAL','CASH_ADVANCE','DIRECT_DEBIT_REVERSAL','BRANCH_DEPOSIT','BRANCH_WITHDRAWAL','CHEQUE_DEPOSIT','CHEQUE_REVERSAL','SEPA_DIRECT_DEBIT','SEPA_TRANSFER','CROSSBORDER_DIRECT_DEBIT','FEE_REVERSAL')) THEN bt.merchant_name
      END AS description
      ,bt.status
      ,bt.migration_date_time
      ,bt.additional_data
      ,bt.card_identification
      ,bt.card_serial_no
      ,bt.creditor_agent_identification
      ,bt.creditor_agent_scheme_name
      ,bt.creditor_scheme_name
      ,bt.debtor_agent_identification
      ,bt.debtor_scheme_name
      ,bt.fee_amount_amount
      ,bt.fee_amount_currency
      ,bt.instrument_sub_type
      ,bt.merchant_name
      ,bt.modification_completed_on
      ,bt.original_transaction_reference
      ,bt.txn_alert_seq_number
      ,bt.card_int_no
      ,bt.event_json
      ,bt.transaction_loaded_at
      ,bt.balance
      ,bt.balance_changed_on
      ,bt.card_id
      ,bt.company_id
      ,bt.debtor_agent_scheme_name
      ,bt.issuer_institution
      ,bt.merchant_location
      ,bt.merchant_identifier
      ,bt.reservation_balance
      ,CURRENT_TIMESTAMP() AS dbt_inserted_at
      ,bt.tis_transaction_type
      ,bt.account_number
      ,bt.sort_code
      ,bt.external_account_number
      ,bt.approval_info
      ,bt.initiation_info
      ,bt.tide_fee_amount_amount
      ,bt.tide_fee_amount_currency
      ,bt.tide_fee_external_tracking_id
      ,IFNULL(fb.new_balance_amount,0) as new_balance_amount
  FROM base_transactions bt
  LEFT JOIN final_balances fb ON bt.tidewallet_transaction_id = fb.tidewallet_transaction_id
)

SELECT *
FROM final