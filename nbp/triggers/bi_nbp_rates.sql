CREATE OR REPLACE TRIGGER bi_nbp_rates
  BEFORE INSERT ON nbp_rates             
  FOR EACH ROW  
BEGIN   
  IF :NEW.id IS NULL THEN 
    SELECT nbp_rates_seq.NEXTVAL INTO :NEW.id FROM dual; 
  END IF; 
END; 