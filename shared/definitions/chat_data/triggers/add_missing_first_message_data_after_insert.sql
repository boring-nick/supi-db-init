DELIMITER //

CREATE TRIGGER `add_missing_first_message_data_after_insert`
AFTER INSERT ON `chat_data`.`Message_Meta_User_Alias`
FOR EACH ROW
BEGIN
	IF (
		NEW.First_Message_Text IS NULL
		AND NEW.First_Message_Posted IS NULL
		AND NEW.Last_Message_Text IS NOT NULL
		AND NEW.Last_Message_Posted IS NOT NULL
	) THEN
		UPDATE chat_data.Message_Meta_User_Alias
        SET
            First_Message_Text = NEW.Last_Message_Text,
            First_Message_Posted = NEW.Last_Message_Posted
        WHERE
            Channel = NEW.Channel
            AND User_Alias = NEW.User_Alias;
	END IF;
END //

DELIMITER ;
