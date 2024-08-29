"""PGSync Trigger template.

This module contains a template for creating a PostgreSQL trigger function that notifies updates asynchronously.
The trigger function constructs a notification as a JSON object and sends it to a channel using PG_NOTIFY.
The notification contains information about the updated table, the operation performed, the old and new rows, and the indices.
"""

from .constants import MATERIALIZED_VIEW, TRIGGER_FUNC, CHANGED_FIELDS

CREATE_BIFROST_TRIGGER_TEMPLATE = f"""
CREATE OR REPLACE FUNCTION {TRIGGER_FUNC}() RETURNS TRIGGER AS $$
DECLARE
  -- database is also the channel name.
  channel TEXT := CURRENT_DATABASE();
  old_row JSON;
  new_row JSON;
  notification JSON;
  xmin BIGINT;
  recorded_at numeric := EXTRACT(EPOCH FROM now()::timestamp);
  error_details JSON;
  _indices TEXT [];
  _primary_keys TEXT [];
  _foreign_keys TEXT [];
  changed_fields JSON;

BEGIN
    BEGIN
        IF TG_OP = 'DELETE' THEN

            SELECT primary_keys, indices
            INTO _primary_keys, _indices
            FROM {MATERIALIZED_VIEW}
            WHERE table_name = TG_TABLE_NAME;

            old_row := (
                SELECT JSONB_OBJECT_AGG(key, value)
                FROM JSONB_EACH(TO_JSONB(OLD))
                WHERE key = ANY(_primary_keys)
            );

            xmin := OLD.xmin;
        ELSE
            IF TG_OP <> 'TRUNCATE' THEN

                SELECT primary_keys, foreign_keys, indices
                INTO _primary_keys, _foreign_keys, _indices
                FROM {MATERIALIZED_VIEW}
                WHERE table_name = TG_TABLE_NAME;

                new_row := (
                    SELECT JSONB_OBJECT_AGG(key, value)
                    FROM JSONB_EACH(TO_JSONB(NEW))
                    WHERE key = ANY(_primary_keys || _foreign_keys)
                );
                IF TG_OP = 'UPDATE' THEN
                    old_row := (
                        SELECT JSONB_OBJECT_AGG(key, value)
                        FROM JSONB_EACH(TO_JSONB(OLD))
                        WHERE key = ANY(_primary_keys || _foreign_keys)
                    );
                    changed_fields := (
                        SELECT JSON_AGG(key)
                        FROM (
                          SELECT * FROM JSONB_EACH(TO_JSONB(NEW))
                            EXCEPT
                              SELECT * FROM JSONB_EACH(TO_JSONB(OLD))
                        ) as t
                    );
                END IF;
                xmin := NEW.xmin;
            END IF;
        END IF;

        -- construct the notification as a JSON object.
        notification = JSON_BUILD_OBJECT(
            'xmin', xmin,
            'new', new_row,
            'old', old_row,
            'indices', _indices,
            'tg_op', TG_OP,
            'table', TG_TABLE_NAME,
            'schema', TG_TABLE_SCHEMA,
            '{CHANGED_FIELDS}', changed_fields
        );

        -- Notify/Listen updates occur asynchronously,
        -- so this doesn't block the Postgres trigger procedure.
        PERFORM PG_NOTIFY(channel, notification::TEXT);

        -- track the changes in business_changes table as well
        insert into bifrost.business_changes (
            transaction_id,
            new_row,
            old_row,
            indices,
            tg_op,
            table_name,
            schema_name,
            changed_fields,
            status,
            recorded_at)
        values (
            xmin,
            new_row,
            old_row,
            to_jsonb(_indices),
            TG_OP,
            TG_TABLE_NAME,
            TG_TABLE_SCHEMA,
            changed_fields,
            'SUCCESS',
            recorded_at);

    EXCEPTION WHEN OTHERS then -- handles all there error exceptions and protects the main transaction from rollback

        error_details = JSON_BUILD_OBJECT
                        ('errorCode', sqlstate,
                         'message', sqlstate || ': ' || sqlerrm);

        BEGIN
            -- track the changes in business_changes table as well
            insert into bifrost.business_changes (
                transaction_id,
                new_row,
                old_row,
                indices,
                tg_op,
                table_name,
                schema_name,
                changed_fields,
                status,
                error_details,
                recorded_at)
            values (
                xmin,
                new_row,
                old_row,
                to_jsonb(_indices),
                TG_OP,
                TG_TABLE_NAME,
                TG_TABLE_SCHEMA,
                changed_fields,
                'ERROR',
                error_details,
                recorded_at);
        EXCEPTION WHEN OTHERS THEN
            RAISE LOG 'ERROR : error occured while processing notification % at %', sqlstate || ': ' || sqlerrm, now();
        END;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql security definer;
"""

CREATE_TRIGGER_TEMPLATE = f"""
CREATE OR REPLACE FUNCTION {TRIGGER_FUNC}() RETURNS TRIGGER AS $$
DECLARE
  channel TEXT;
  old_row JSON;
  new_row JSON;
  notification JSON;
  xmin BIGINT;
  _indices TEXT [];
  _primary_keys TEXT [];
  _foreign_keys TEXT [];
  {CHANGED_FIELDS} JSON;

BEGIN
    BEGIN
        -- database is also the channel name.
        channel := CURRENT_DATABASE();

        IF TG_OP = 'DELETE' THEN

            SELECT primary_keys, indices
            INTO _primary_keys, _indices
            FROM {MATERIALIZED_VIEW}
            WHERE table_name = TG_TABLE_NAME;

            old_row = ROW_TO_JSON(OLD);
            old_row := (
                SELECT JSONB_OBJECT_AGG(key, value)
                FROM JSON_EACH(old_row)
                WHERE key = ANY(_primary_keys)
            );
            xmin := OLD.xmin;
        ELSE
            IF TG_OP <> 'TRUNCATE' THEN

                SELECT primary_keys, foreign_keys, indices
                INTO _primary_keys, _foreign_keys, _indices
                FROM {MATERIALIZED_VIEW}
                WHERE table_name = TG_TABLE_NAME;

                new_row = ROW_TO_JSON(NEW);
                new_row := (
                    SELECT JSONB_OBJECT_AGG(key, value)
                    FROM JSON_EACH(new_row)
                    WHERE key = ANY(_primary_keys || _foreign_keys)
                );
                IF TG_OP = 'UPDATE' THEN
                    old_row = ROW_TO_JSON(OLD);
                    old_row := (
                        SELECT JSONB_OBJECT_AGG(key, value)
                        FROM JSON_EACH(old_row)
                        WHERE key = ANY(_primary_keys || _foreign_keys)
                    );
                    {CHANGED_FIELDS} := (
                        SELECT JSON_AGG(key)
                        FROM (
                          SELECT * FROM JSONB_EACH(TO_JSONB(NEW))
                            EXCEPT
                              SELECT * FROM JSONB_EACH(TO_JSONB(OLD))
                        ) as t
                    );
                END IF;
                xmin := NEW.xmin;
            END IF;
        END IF;

        -- construct the notification as a JSON object.
        notification = JSON_BUILD_OBJECT(
            'xmin', xmin,
            'new', new_row,
            'old', old_row,
            'indices', _indices,
            'tg_op', TG_OP,
            'table', TG_TABLE_NAME,
            'schema', TG_TABLE_SCHEMA,
            '{CHANGED_FIELDS}', {CHANGED_FIELDS}
        );

        -- Notify/Listen updates occur asynchronously,
        -- so this doesn't block the Postgres trigger procedure.
        PERFORM PG_NOTIFY(channel, notification::TEXT);

    EXCEPTION WHEN OTHERS THEN -- handles all there error exceptions and protects the main transaction from rollback
        RAISE LOG 'ERROR : error occured while processing notification % at %', sqlstate || ': ' || sqlerrm, now();
    END;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
