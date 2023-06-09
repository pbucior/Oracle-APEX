CREATE OR REPLACE TRIGGER bi_nbp_header
  BEFORE INSERT ON nbp_header               
  FOR EACH ROW  
BEGIN   
  IF :NEW.id IS NULL THEN 
    SELECT nbp_header_seq.NEXTVAL INTO :NEW.id FROM dual; 
  END IF; 
END;