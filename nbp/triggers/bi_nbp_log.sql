CREATE OR REPLACE TRIGGER bi_nbp_log
  BEFORE INSERT ON nbp_log             
  FOR EACH ROW  
BEGIN   
  IF :NEW.id IS NULL THEN 
    SELECT nbp_log_seq.NEXTVAL INTO :NEW.ID FROM dual; 
  END IF; 
END;