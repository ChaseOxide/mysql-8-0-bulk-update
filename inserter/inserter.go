package inserter

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
)

type KeyValue struct {
	Key   string
	Value interface{}
}

type UpdateInstr struct {
	SetInstr   []KeyValue
	WhereInstr []KeyValue
}

func GenerateUpdateInstr(count int, collisionRate float32) []UpdateInstr {
	instrs := []UpdateInstr{}

	for i := 0; i < count; i++ {
		instrs = append(instrs, UpdateInstr{
			SetInstr: []KeyValue{
				{"unit", randCollision(count, collisionRate)},
			},
			WhereInstr: []KeyValue{
				{"age", randCollision(count, collisionRate)},
			},
		})
	}

	return instrs
}

func GenerateUpdateInstrWithId(count int, collisionRate float32) []UpdateInstr {
	instrs := []UpdateInstr{}
	for i := 0; i < count; i++ {
		instrs = append(instrs, UpdateInstr{
			SetInstr: []KeyValue{
				{"unit", randCollision(count, collisionRate)},
			},
			WhereInstr: []KeyValue{
				{"id", randCollision(count, collisionRate) + 1},
			},
		})
	}

	return instrs
}

type connection interface {
	Prepare(query string) (*sql.Stmt, error)
}

func AtomUpdate(db connection, tableName string, instrs []UpdateInstr) error {
	for _, instr := range instrs {
		stmt, err := db.Prepare(
			fmt.Sprintf(
				"UPDATE `%s` SET %s WHERE %s",
				tableName,
				strings.Join(mapToKeyAssignmentList(instr.SetInstr), ","),
				strings.Join(mapToKeyAssignmentList(instr.WhereInstr), " AND "),
			),
		)
		if err != nil {
      return err
		}

		_, err = stmt.Exec(
			append(
				mapToValueList(instr.SetInstr),
				mapToValueList(instr.WhereInstr)...,
			)...,
		)
		if err != nil {
      return err
		}
	}

  return nil
}

type statementArgs struct {
	statement string
	args      []interface{}
}

func CaseUpdate(db *sql.DB, tableName string, instrs []UpdateInstr) error {
	expressionsMap := map[string][]statementArgs{}
	whereClauses := []statementArgs{}

	for _, instr := range instrs {
		var whereClauseStmt strings.Builder
		whereClauseArgs := []interface{}{}

		for i, kv := range instr.WhereInstr {
			if i > 0 {
				fmt.Fprint(&whereClauseStmt, " AND")
			}
			fmt.Fprintf(&whereClauseStmt, "`%s` = ?", kv.Key)
			whereClauseArgs = append(whereClauseArgs, kv.Value)
		}

		whereClause := statementArgs{
			whereClauseStmt.String(),
			whereClauseArgs,
		}
		whereClauses = append(whereClauses, whereClause)

		for _, kv := range instr.SetInstr {
			expressionsMap[kv.Key] = append(expressionsMap[kv.Key], statementArgs{
				fmt.Sprintf(" WHEN %s THEN ?", whereClause.statement),
				append(whereClause.args, kv.Value),
			})
		}
	}

	var query strings.Builder
	args := []interface{}{}

	fmt.Fprintf(&query, "UPDATE `%s` SET ", tableName)

	for k, expressions := range expressionsMap {
		fmt.Fprintf(&query, "`%s` = CASE", k)
		for _, expression := range expressions {
			fmt.Fprint(&query, expression.statement)
			args = append(args, expression.args...)
		}
		fmt.Fprint(&query, " END")
	}

	fmt.Fprint(&query, " WHERE ")

	for i, whereClause := range whereClauses {
		if i > 0 {
			fmt.Fprint(&query, " OR")
		}

		fmt.Fprintf(&query, " (%s)", whereClause.statement)
		args = append(args, whereClause.args...)
	}

	stmt, err := db.Prepare(query.String())
	if err != nil {
    return err
	}

	_, err = stmt.Exec(args...)

  return err
}

func JoinUpdate(db *sql.DB, tableName string, instrs []UpdateInstr) error {
	var statement strings.Builder
	args := []interface{}{}

	setColMap := map[string]bool{}
	whereColMap := map[string]bool{}

	fmt.Fprintf(&statement, "UPDATE `%s` INNER JOIN (", tableName)

	for i, instr := range instrs {
		if i > 0 {
			fmt.Fprint(&statement, " UNION ALL ")
		}

		fmt.Fprint(&statement, "SELECT ")

		for i, kv := range instr.SetInstr {
			if i > 0 {
				fmt.Fprint(&statement, ", ")
			}
			fmt.Fprintf(&statement, "? `s$%s`", kv.Key)
			args = append(args, kv.Value)
			setColMap[kv.Key] = true
		}
		fmt.Fprint(&statement, ", ")
		for i, kv := range instr.WhereInstr {
			if i > 0 {
				fmt.Fprint(&statement, ", ")
			}
			fmt.Fprintf(&statement, "? `w$%s`", kv.Key)
			args = append(args, kv.Value)
			whereColMap[kv.Key] = true
		}
	}

	fmt.Fprint(&statement, ") `$u` ON")

  counter := 0
	for col := range whereColMap {
		if counter > 0 {
			fmt.Fprint(&statement, ",")
		}
		fmt.Fprintf(&statement, "`%s`.`%s` = `$u`.`w$%s`", tableName, col, col)
		counter += 1
	}

  fmt.Fprint(&statement, " SET ")

	counter = 0
	for col := range setColMap {
		if counter > 0 {
			fmt.Fprint(&statement, " AND")
		}
		fmt.Fprintf(&statement, "`%s`.`%s` = `$u`.`s$%s`", tableName, col, col)
		counter += 1
	}

	stmt, err := db.Prepare(statement.String())
	if err != nil {
    return err
	}

  _, err = stmt.Exec(args...)
  
  return err
}

func randCollision(count int, collisionRate float32) int {
	return rand.Intn(int(float32(count)*(1-collisionRate)) + 1)
}

func mapToKeyAssignmentList(instrs []KeyValue) []string {
	result := make([]string, len(instrs))

	for i, instr := range instrs {
		result[i] = fmt.Sprintf("%s = ?", instr.Key)
	}

	return result
}

func mapToValueList(instrs []KeyValue) []interface{} {
	result := make([]interface{}, len(instrs))

	for i, instr := range instrs {
		result[i] = instr.Value
	}

	return result
}
