package bootstrap

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
)

func RecreateTable(db *sql.DB, tableName string) {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(
		fmt.Sprintf(
			"CREATE TABLE `%s` ("+
				"  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"+
				"  age INT NOT NULL,"+
				"  unit INT NOT NULL"+
				")",
			tableName),
	)
	if err != nil {
		panic(err)
	}
}

func GenerateInsertQuery(tableName string, count int, collisionRate float32) string {
	values := []string{}
	for i := 0; i < count; i++ {
		values = append(values, fmt.Sprintf(
			"(%d, %d)",
			rand.Intn(int(float32(count)*(1-collisionRate))+1),
			0,
		))
	}

	return fmt.Sprintf(
		"INSERT INTO `%s` (age, unit) VALUES %s",
		tableName,
		strings.Join(values, ",\n"),
	)
}
