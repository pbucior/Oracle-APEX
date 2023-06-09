CREATE OR REPLACE TRIGGER nbp_header_t1
FOR INSERT ON nbp_header
COMPOUND TRIGGER

    TYPE NumberArray IS TABLE OF NUMBER;
    myArray NumberArray := NumberArray();

	AFTER EACH ROW IS
	BEGIN
		myArray.extend;
		myArray(myArray.LAST) := :NEW.id;
	END AFTER EACH ROW;


	AFTER STATEMENT IS
	BEGIN
		FOR idx in myArray.FIRST .. myArray.LAST
		LOOP
			nbp.insertCurrencyRates(myArray(idx));
		END LOOP;
		myArray.DELETE;
	END AFTER STATEMENT;
END;