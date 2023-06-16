package main

import (
	"fmt"
)

func createTable() error {
	_, err := db.Exec(dbCreateQuery)
	if err != nil {
		return err
	}
	return nil
}

func existsInDB(n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit string) bool {

	var count int
	existingInDB := fmt.Sprintf(dbSelectQuery, n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit)

	row := db.QueryRow(existingInDB)
	if err := row.Scan(&count); err != nil {
		fmt.Println(err)
		return false
	}

	return count > 0
}

func insertInDB(n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit string) error {
	insertingInDB := fmt.Sprintf(dbInsertQuery, n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit)

	_, err := db.Exec(insertingInDB)
	if err != nil {
		return err
	}

	return nil
}

func updateInDB(n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit string) error {
	updatingInDB := fmt.Sprintf(dbUpdateQuery, n, mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type_table, bit, invert_bit)
	_, err := db.Exec(updatingInDB)
	if err != nil {
		return err
	}

	return nil
}
