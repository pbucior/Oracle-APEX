CREATE OR REPLACE TRIGGER bi_nbp_currency
  BEFORE INSERT ON nbp_currency             
  FOR EACH ROW  
BEGIN   
  IF :NEW.id IS NULL THEN 
    SELECT nbp_currency_seq.NEXTVAL INTO :NEW.id FROM dual; 
  END IF; 
END;