CREATE OR REPLACE PACKAGE nbp as
	PROCEDURE getNbpHeaders(startDt IN DATE, endDt IN DATE);
    PROCEDURE getNbpHeader(dt IN DATE);
    PROCEDURE getNbpToday;
    PROCEDURE insertCurrencyRates(headerId IN NUMBER);
    FUNCTION insertCurrency(p_code IN VARCHAR2)
        RETURN NUMBER;
    FUNCTION getCurrencyId(p_code IN VARCHAR2)
        RETURN NUMBER;
    PROCEDURE addLog(p_message IN VARCHAR2);
    
    FUNCTION getRatesForCurrency(currencyId IN NUMBER, pastDays IN NUMBER)
        RETURN CLOB;
END;
/


CREATE OR REPLACE PACKAGE BODY nbp AS
    PROCEDURE getNbpHeaders(startDt IN DATE, endDt IN DATE)
    IS
        TYPE NbpHeaderRecord IS RECORD
        (
            tab VARCHAR2(100),
            no  VARCHAR2(100),
            effectiveDate DATE
        );

        TYPE NbpHeaderTable IS TABLE OF NbpHeaderRecord INDEX BY BINARY_INTEGER;
        nbpHeaderDataSet NbpHeaderTable;
        v_url VARCHAR2(1000) := 'https://api.nbp.pl/api/exchangerates/tables/a/' ||
                                TO_CHAR(startDt, 'YYYY-MM-DD') ||
                                '/' ||
                                TO_CHAR(endDt, 'YYYY-MM-DD') ||
                                '?format=json';
        v_json CLOB;
    BEGIN
        v_json := apex_web_service.make_rest_request(
            p_url => v_url,
            p_http_method => 'GET'
        );

        SELECT tab, no, TO_DATE(effectiveDate, 'YYYY-MM-DD')
        BULK COLLECT INTO nbpHeaderDataSet
        FROM XMLTABLE('/json/row' PASSING apex_json.to_xmltype(v_json) COLUMNS
                tab VARCHAR2(100) PATH 'table',
                no VARCHAR2(100) PATH 'no',
                effectiveDate VARCHAR2(100) PATH 'effectiveDate');

        FORALL idx IN nbpHeaderDataSet.FIRST .. nbpHeaderDataSet.LAST
            INSERT INTO nbp_header (tab, no, effective_date)
                VALUES(nbpHeaderDataSet(idx).tab, nbpHeaderDataSet(idx).no, nbpHeaderDataSet(idx).effectiveDate);

    END;

    PROCEDURE getNbpHeader(dt IN DATE)
    IS
        tab VARCHAR2(100);
        no  VARCHAR2(100);
        effectiveDate DATE;
        v_url VARCHAR2(1000) := 'https://api.nbp.pl/api/exchangerates/tables/a/' ||
                                TO_CHAR(dt, 'YYYY-MM-DD') ||
                                '?format=json';
        v_json CLOB;
        isNew NUMBER;
    BEGIN
        v_json := apex_web_service.make_rest_request(
            p_url => v_url,
            p_http_method => 'GET'
        );

        IF SUBSTR(v_json, 1, 3) != '404' THEN
            SELECT tab, no, TO_DATE(effectiveDate, 'YYYY-MM-DD')
            INTO tab, no, effectiveDate
            FROM XMLTABLE('/json/row' PASSING apex_json.to_xmltype(v_json) COLUMNS
                    tab VARCHAR2(100) PATH 'table',
                    no VARCHAR2(100) PATH 'no',
                    effectiveDate VARCHAR2(100) PATH 'effectiveDate');

            SELECT COUNT(*) INTO isNew FROM nbp_header WHERE effective_date = effectiveDate;

            IF isNew = 0 THEN
                INSERT INTO nbp_header (tab, no, effective_date, json_file, is_inserted)
                    VALUES(tab, no, effectiveDate, v_json, 0);
            END IF;
        END IF;
    END;

    PROCEDURE getNbpToday
    IS
        tab VARCHAR2(100);
        no  VARCHAR2(100);
        effectiveDate DATE;
        v_url VARCHAR2(1000) := 'https://api.nbp.pl/api/exchangerates/tables/a/today?format=json';
        v_json CLOB;
        isNew NUMBER;
    BEGIN
        v_json := apex_web_service.make_rest_request(
            p_url => v_url,
            p_http_method => 'GET'
        );

        IF SUBSTR(v_json, 1, 3) != '404' THEN
            SELECT tab, no, TO_DATE(effectiveDate, 'YYYY-MM-DD')
            INTO tab, no, effectiveDate
            FROM XMLTABLE('/json/row' PASSING apex_json.to_xmltype(v_json) COLUMNS
                    tab VARCHAR2(100) PATH 'table',
                    no VARCHAR2(100) PATH 'no',
                    effectiveDate VARCHAR2(100) PATH 'effectiveDate');

            SELECT COUNT(*) INTO isNew FROM nbp_header WHERE effective_date = effectiveDate;

            IF isNew = 0 THEN
                INSERT INTO nbp_header (tab, no, effective_date, json_file, is_inserted)
                    VALUES(tab, no, effectiveDate, v_json, 0);
                nbp.addLog('Dodano ' || no || ' - ' || effectiveDate);
            ELSE
                nbp.addLog('Ju¿ jest w bazie ' || no || ' - ' || effectiveDate);
            END IF; 
        ELSE
            nbp.addLog('B³¹d 404');
        END IF;
    END;

    PROCEDURE insertCurrencyRates(headerId IN NUMBER)
    IS
        v_json CLOB;
        
        currencyId NUMBER;
        code VARCHAR2(100);
        mid NUMBER;
    BEGIN
        SELECT json_file INTO v_json
            FROM nbp_header
            WHERE id = headerId;

        APEX_JSON.parse(v_json);

        FOR i IN 1 .. APEX_JSON.get_count(p_path => '[1].rates')
        LOOP
            code := APEX_JSON.get_varchar2(p_path => '[1].rates[%d].code', p0 => i);
            mid := APEX_JSON.get_number(p_path => '[1].rates[%d].mid', p0 => i);
            currencyId := nbp.getCurrencyId(code);
            IF currencyId = 0 THEN
                currencyId := nbp.insertCurrency(code);
            END IF;

            INSERT INTO nbp_rates(header_id, currency_id, mid)
                VALUES(headerId, currencyId, mid);
        END LOOP;

        UPDATE nbp_header SET is_inserted = 1 WHERE id = headerId;

        nbp.addLog('Dodano kursy dla headerId = ' || headerId);
    END;

    PROCEDURE insertCurrencyOld(p_country IN VARCHAR2, p_code IN VARCHAR2)
    IS
    BEGIN
        INSERT INTO nbp_currency (country, code)
            VALUES (p_country, p_code);
    END;

    FUNCTION insertCurrency(p_code IN VARCHAR2)
        RETURN NUMBER
    IS
        currencyId NUMBER;
    BEGIN
        INSERT INTO nbp_currency (code)
            VALUES (p_code)
            RETURNING id INTO currencyId;

        RETURN currencyId;
    END;

    FUNCTION getCurrencyId(p_code IN VARCHAR2)
        RETURN NUMBER
    IS
        currencyId NUMBER;
    BEGIN
        SELECT id INTO currencyId
            FROM nbp_currency
            WHERE code = p_code;

        RETURN currencyId;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN 0;
    END;

    PROCEDURE addLog(p_message IN VARCHAR2)
    IS
    BEGIN
        INSERT INTO nbp_log (data, message)
            VALUES (SYSDATE + 2/24, p_message);
    END;
    
    FUNCTION getRatesForCurrency(currencyId IN NUMBER, pastDays IN NUMBER)
        RETURN CLOB
    IS
        sqlQuery CLOB;
    BEGIN
        IF pastDays = -1 THEN
            sqlQuery := 'SELECT
                            TO_CHAR(h.effective_date, ''YYYY-MM-DD''),
                            h.no,
                            c.code,
                            r.mid
                        FROM nbp_rates r
                            JOIN nbp_header h ON r.header_id = h.id
                            JOIN nbp_currency c ON r.currency_id = c.id
                        WHERE c.id = ' || currencyId ||
                        ' ORDER BY h.effective_date DESC';
        ELSE
            sqlQuery := 'SELECT
                            TO_CHAR(h.effective_date, ''YYYY-MM-DD''),
                            h.no,
                            c.code,
                            r.mid
                        FROM nbp_rates r
                            JOIN nbp_header h ON r.header_id = h.id
                            JOIN nbp_currency c ON r.currency_id = c.id
                        WHERE c.id = ' || currencyId ||
                            'AND h.effective_date >= (SELECT MAX(head.effective_date)
                                                        FROM nbp_header head
                                                        JOIN nbp_rates rates ON head.id = rates.header_id
                                                        WHERE rates.currency_id = c.id) - ' || pastDays ||
                        ' ORDER BY h.effective_date DESC';
        END IF;
        
        RETURN sqlQuery;
    END;
    
END;
/
