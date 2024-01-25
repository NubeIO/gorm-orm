package orm

import (
	"errors"
	"fmt"
	"github.com/NubeIO/rubix-rx/server/database"
	"github.com/NubeIO/rubix-rx/server/database/response"
	"github.com/go-playground/validator/v10"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"reflect"
	"strings"
)

var (
	errNoWhereClause = gorm.ErrMissingWhereClause
)

func GetType(model interface{}) string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct {
		// Get the name of the struct from the full type name
		typeName := t.String()
		parts := strings.Split(typeName, ".")
		return parts[len(parts)-1]
	}
	return "Not a struct"
}

// PaginatedResult holds the result of a paginated query.
type PaginatedResult struct {
	Results    any   `json:"results"`    // Pointer to a slice of models
	Count      int64 `json:"count"`      // Total number of results
	TotalPages int   `json:"totalPages"` // Total number of pages
	HasNext    bool  `json:"hasNext"`    // Indicates if there are more pages
	HasPrev    bool  `json:"hasPrev"`    // Indicates if there are previous pages
	Page       int   `json:"page"`       // Current page
}

// ORM interface defines operations for creating, reading, updating, and deleting entities.
type ORM interface {
	Get(model any, where *WhereClause, preload []string) *response.ErrorResponse
	All(model any, where *WhereClause) *response.ErrorResponse
	GetAllPaginated(models any, page int, pageSize int, where *WhereClause) (*PaginatedResult, *response.ErrorResponse)
	Create(model any) []*response.ErrorResponse
	Update(updates any, uuid string, where *WhereClause) (any, *response.ErrorResponse)
	Delete(model any, where *WhereClause) (bool, *response.ErrorResponse, int64) //int64 is deleted count
	BulkCreate(models []interface{}) []*response.ErrorResponse
	BulkUpdate(models []interface{}, uuids []string, where *WhereClause) []*response.ErrorResponse
	BulkDelete(modelType any, uuids []string, where *WhereClause) []*response.ErrorResponse
	GetDB() *gorm.DB
	Migrate(models ...interface{}) error
}

var resp *response.ErrorResponse

type orm struct {
	DB       *gorm.DB
	resp     *response.ErrorResponse
	validate *validator.Validate
}

func New(c *database.DBConfig, resp *response.ErrorResponse) ORM {
	db, err := database.Connect(c)
	if err != nil {
		panic(err) // Consider returning an error instead
	}

	return &orm{
		DB:       db,
		resp:     resp,
		validate: validator.New(),
	}
}

func GetErrorResponse() *response.ErrorResponse {
	return resp
}

func (o *orm) Get(model any, where *WhereClause, preload []string) *response.ErrorResponse {
	query := o.DB
	for _, preloadAssociation := range preload {
		query = query.Preload(preloadAssociation)
	}

	if where == nil {
		return o.resp.New(gorm.ErrMissingWhereClause, model)
	}

	result := query.Where(where.Query, where.Args...).First(model)
	return o.resp.New(result.Error, model)
}

func (o *orm) All(models any, where *WhereClause) *response.ErrorResponse {
	query := o.DB
	if where != nil {
		preload := where.Preload
		for _, preloadAssociation := range preload {
			query = query.Preload(preloadAssociation)
		}

		if where.Limit > 0 {
			query = query.Limit(where.Limit)
		}
		if where.Offset > 0 {
			query = query.Offset(where.Offset)
		}

		if where.OrderByASC != "" {
			// Handle ordering based on the orderBy parameter
			query = query.Order(fmt.Sprintf("%s ASC", where.OrderByASC))
		}

		if where.OrderByDESC != "" {
			// Handle ordering based on the orderBy parameter
			query = query.Order(fmt.Sprintf("%s DESC", where.OrderByDESC))
		}

		result := query.Where(where.Query, where.Args...).Find(models)
		return o.resp.New(result.Error, models)
	}

	result := query.Find(models)
	return o.resp.New(result.Error, models)
}

func (o *orm) GetAllPaginated(models any, page int, pageSize int, where *WhereClause) (*PaginatedResult, *response.ErrorResponse) {
	var count int64
	db := o.DB.Model(models)
	if where != nil {
		db = db.Where(where.Query, where.Args...)
	}
	err := db.Count(&count).Error
	if err != nil {
		return nil, o.resp.New(err, models)
	}

	totalPages := int(count) / pageSize
	if int(count)%pageSize > 0 {
		totalPages++
	}

	offset := (page - 1) * pageSize
	if where != nil {
		err = db.Offset(offset).Limit(pageSize).Find(models).Error
	} else {
		err = db.Offset(offset).Limit(pageSize).Find(models).Error
	}

	return &PaginatedResult{
		Results:    models,
		Count:      count,
		TotalPages: totalPages,
		HasNext:    page < totalPages,
		HasPrev:    page > 1,
		Page:       page,
	}, o.resp.New(err, models)
}

func (o *orm) Create(model any) []*response.ErrorResponse {
	var respErrors []*response.ErrorResponse
	if err := o.validate.Struct(model); err != nil {
		for _, err := range err.(validator.ValidationErrors) {
			newErr := errors.New(fmt.Sprintf("Field: %s, ErrorMessage: %s\n", err.StructField(), err.Tag()))
			n := o.resp.NewValidation(newErr)
			respErrors = append(respErrors, n)
		}
		return respErrors
	}
	err := o.DB.Create(model).Error
	if err != nil {
		respErrors = append(respErrors, o.resp.New(err, model))
		return respErrors
	}
	r := o.DB.Preload(clause.Associations).First(model).Error
	if r != nil {
		respErrors = append(respErrors, o.resp.New(r, model))
		return respErrors
	}
	return nil
}

func (o *orm) Update(updates any, uuid string, where *WhereClause) (any, *response.ErrorResponse) {
	if err := o.validate.Struct(updates); err != nil {
		return nil, o.resp.New(err, updates)
	}
	entity := GetType(updates)
	if where == nil {
		return nil, o.resp.New(gorm.ErrMissingWhereClause, updates)
	}

	err := o.DB.First(entity, uuid).Error
	if err != nil {
		return nil, o.resp.New(err, updates)
	}

	err = o.DB.Model(entity).Where(where.Query, where.Args...).Updates(updates).Error
	if err != nil {
		return nil, o.resp.New(err, updates)
	}

	err = o.DB.Preload(clause.Associations).Where(where.Query, where.Args...).First(entity).Error
	return entity, o.resp.New(err, updates)
}

func (o *orm) Delete(model any, where *WhereClause) (bool, *response.ErrorResponse, int64) {
	if where == nil {
		return false, o.resp.New(gorm.ErrMissingWhereClause, model), 0
	}
	r := o.DB.Where(where.Query, where.Args...).Delete(model)
	if r.Error != nil {
		return false, o.resp.ErrorDeletion(r.Error, model), 0
	} else if r.RowsAffected == 0 {
		return false, o.resp.ErrorDeletion(r.Error, model), 0
	}
	return true, nil, r.RowsAffected

}

// BULK

func (o *orm) BulkCreate(models []interface{}) []*response.ErrorResponse {
	var respErrors []*response.ErrorResponse
	tx := o.DB.Begin()
	for _, model := range models {
		if err := o.validate.Struct(model); err != nil {
			respErrors = append(respErrors, o.resp.New(err, model))
			return respErrors
		}
		if err := tx.Create(model).Error; err != nil {
			respErrors = append(respErrors, o.resp.New(err, model))
			tx.Rollback()
			return respErrors
		}
	}
	if err := tx.Commit().Error; err != nil {
		respErrors = append(respErrors, o.resp.New(err, nil))
	}
	return respErrors
}

func (o *orm) BulkUpdate(models []interface{}, uuids []string, where *WhereClause) []*response.ErrorResponse {
	var respErrors []*response.ErrorResponse
	if len(models) != len(uuids) {
		return append(respErrors, o.resp.New(errors.New("mismatch in models and uuids length"), nil))
	}

	tx := o.DB.Begin()
	for i, model := range models {
		if where == nil {
			return append(respErrors, o.resp.New(errNoWhereClause, nil))
		}
		if err := tx.Model(model).Where("uuid = ?", uuids[i]).Where(where.Query, where.Args...).Updates(model).Error; err != nil {
			respErrors = append(respErrors, o.resp.New(err, model))
			tx.Rollback()
			return respErrors
		}
	}
	if err := tx.Commit().Error; err != nil {
		respErrors = append(respErrors, o.resp.New(err, nil))
	}
	return respErrors
}

func (o *orm) BulkDelete(modelType any, uuids []string, where *WhereClause) []*response.ErrorResponse {
	var respErrors []*response.ErrorResponse
	if where == nil {
		return append(respErrors, o.resp.New(errNoWhereClause, nil))
	}

	tx := o.DB.Begin()
	for _, uuid := range uuids {
		entity := GetType(modelType)
		if err := tx.Where("uuid = ?", uuid).Where(where.Query, where.Args...).Delete(entity).Error; err != nil {
			respErrors = append(respErrors, o.resp.New(err, entity))
			tx.Rollback()
			return respErrors
		}
	}
	if err := tx.Commit().Error; err != nil {
		respErrors = append(respErrors, o.resp.New(err, nil))
	}
	return respErrors
}

func (o *orm) GetDB() *gorm.DB {
	return o.DB
}

func (o *orm) Migrate(models ...interface{}) error {
	return o.DB.AutoMigrate(models...)
}
