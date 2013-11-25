<?php
/**
 *
 * @author isdarka
 * @created Nov 22, 2013 5:11:59 PM
 */

namespace Query;

use Zend\Db\Sql\Select;
use Zend\Db\Adapter\Adapter;
use Zend\Db\Sql\Where;
use Zend\Db\Sql\Predicate\Predicate;
use Query\Exception\QueryException;
use Query\Interfaces\Comparision;
use Zend\Db\ResultSet\ResultSet;
use Zend\Db\Sql\Sql;
use Zend\Db\Sql\Expression;
use Model\Bean\AbstractBean;
use Model\Collection\AbstractCollection;
// use Core\Model\Metadata\ModuleMetadata as ModuleMetadata;

class Query extends Select implements Comparision
{
	/** @var $metadata Model\Metadata\AbstractMetadata */
	protected $metadata;
	
	/** @var $adapter Zend\Db\Adapter\Adapter */
	private $adapter;
	
	/** @var $adapter string */
	private $entityName;
	
	private $predicate = null;
	
	/** @var $where Zend\Db\Sql\Where */
// 	protected $where;
	
	/**
	 * 
	 * @param Adapter $adapter
	 * @param unknown $tableName
	 * @param string $entity
	 */
	public function __construct(Adapter $adapter, $tableName, $entityName = null)
	{
		$this->adapter = $adapter;
		if(is_null($entityName))
			$entityName = $tableName;
		
		$this->entityName = $entityName;
		$this->where = new Where();
		parent::__construct(array($entityName => $tableName));
	}
	
	/**
	 * Get SQL string for statement
	 * @return string
	 */
	public function toSql()
	{
		return $this->getSqlString($this->adapter->getPlatform());
	}
	
	/**
	 * Add WHERE AND clausule
	 * @param unknown $field
	 * @param unknown $value
	 * @param unknown $comparision
	 * @return \Query\Query
	 */
	public function whereAdd($field, $value, $comparision = self::EQUAL)
	{
		$this->predicate($field, $value,$comparision, Predicate::COMBINED_BY_AND);
		return $this;
	}
	
	/**
	 * Add WHERE OR clausule
	 * @param unknown $field
	 * @param unknown $value
	 * @param unknown $comparision
	 * @return \Query\Query
	 */
	public function whereOrAdd($field, $value, $comparision = self::EQUAL)
	{
		$this->predicate($field, $value,$comparision, Predicate::COMBINED_BY_OR);
		return $this;
	}
	
	/**
	 * 
	 * @param unknown $field
	 * @param unknown $value
	 * @param unknown $comparision
	 * @param unknown $combination
	 * @throws QueryException
	 */
	private function predicate($field, $value, $comparision, $combination)
	{
		if($combination == Predicate::COMBINED_BY_AND)
			$this->predicate = new Predicate(null, Predicate::COMBINED_BY_AND);
		else
			$this->predicate->__get("or");
	
		switch ($comparision)
		{
			case self::IN:
				if(!is_array($value))
					throw new QueryException('$value must be array but is '.gettype($value));
				$this->predicate->in($this->entityName . "." . $field, $value);
				break;
			case self::EQUAL:
				$this->predicate->equalTo($this->entityName . "." . $field, $value);
				break;
			case self::BETWEEN:
				if(!is_array($value))
					throw new QueryException('$value must be array but is '.gettype($value));
				$this->predicate->between($field, $value[0], $value[1]);
				break;
			default:
				$this->predicate->equalTo($field, $value, $comparision);
				break;
		}
		if($combination == Predicate::COMBINED_BY_AND)
			$this->where->addPredicate($this->predicate);
	
	}
	
	
	/**
	 * Specify columns from which to select
	 * @param unknown $field
	 * @param string $alias
	 * @param string $mutator
	 * @return \Query\Query
	 */
	public function addColumn($field, $alias = null, $mutator = null)
	{
		if(!empty($this->columns))
			$this->columns = array();
	
		if($field instanceof Query)
		{
			$this->columns[$alias] = new Expression(sprintf(("(%s)"), $field->toSql()));
		}else{
			if(is_null($mutator) && !is_null($alias))
				$this->columns[$alias] = $field;
			elseif (is_null($alias))
			$this->columns[$field] = $field;
			else
				$this->columns[$alias] = new Expression(sprintf($mutator, $field));
		}
		return $this;
	}
	
	/**
	 * Add Group By field
	 * @param string $field
	 */
	public function addGroupBy($field)
	{
		$this->group($field);
	}
	
	/**
	 * Order descendent data by field
	 * @param string $field
	 */
	public function addDescendingOrderBy($field)
	{
		$this->order[$field] = self::DESC;
	}
	
	/**
	 * Order ascendent data by field
	 * @param string $field
	 */
	public function addAscendingOrderBy($field)
	{
		$this->order[$field] = self::ASC;
	}
	
	/**
	 * @return array
	 */
	public function fecthAll()
	{
		return $this->resultSet()->toArray();
	}
	
	/**
	 * Return one row
	 * @return Ambigous <multitype:, ArrayObject, NULL, \ArrayObject, unknown>
	 */
	public function fetchOne()
	{
		/* @var $this->resultSet() Zend\Db\ResultSet\ResultSet */
		return $this->resultSet()->current();
	}
	
	/**
	 * Return count of rowsCount elements of an object
	 * @return number
	 */
	public function count()
	{
		return (int) $this->resultSet()->count();
	}
	
	/**
	 * 
	 * @return Zend\Db\ResultSet\ResultSet
	 */
	private function resultSet()
	{
		$resulSet = new ResultSet();
		return $resulSet->initialize($this->getStatement());
	}
	
	/**
	 * 
	 * @return \Zend\Db\Adapter\Driver\ResultInterface
	 */
	private function getStatement()
	{
		$sql = new Sql($this->adapter);
		return $sql->prepareStatementForSqlObject($this)->execute();
	}
	
	public function innerJoin(AbstractBean $bean)
	{
		$this->join(
					array($this->getBeanMetadata($bean)->getEntityName() => $this->getBeanMetadata($bean)->getTableName()),
					$this->metadata->getEntityName().".".$this->getBeanMetadata($bean)->getPrimaryKey().
					"=".
					$this->getBeanMetadata($bean)->getEntityName().
					".".
					$this->getBeanMetadata($bean)->getPrimaryKey(),
					self::SQL_STAR,
					self::JOIN_INNER
				);
		return $this;
	}
	
	private function getBeanMetadata(AbstractBean $bean)
	{
		$bean = explode('\\', get_class($bean));
		$metadata = substr($bean[3], 0, -4)."Metadata";
		$bean[2] = "Metadata";
		$bean[3] = $metadata;
		$metadata = implode('\\', $bean);

		if(!class_exists($metadata))
			throw new QueryException($metadata.' not found');
		
		return new $metadata();
		
	}
	
	
	
	/**
	 *
	 * @return AbstractCollection
	 */
	public function find()
	{
		$array = $this->fecthAll();
		$collection = $this->metadata->newCollection();
		foreach ($array as $item)
			$collection->append($this->metadata->getFactory()->createFromArray($item));
	
		return $collection;
	}
	
	/**
	 * @return AbstractBean
	 */
	public function findOne()
	{
		$array = $this->fetchOne();
		return $this->metadata->getFactory()->createFromArray($array);
	}
	
	/**
	 * 
	 * @param int $primaryKey
	 */
	public function findByPk($primaryKey)
	{
		if(is_null($primaryKey) || empty($primaryKey))
			throw new QueryException("Primary key not defined");
		$this->whereAdd($this->metadata->getPrimaryKey(), (int) $primaryKey, Comparision::EQUAL);
		return $this->find()->getOne();
	}
	
	public function findByPkOrThrow($primaryKey, $exception)
	{
		if($this->findByPk($primaryKey) instanceof AbstractBean == false)
			throw new QueryException($exception);
		else 
			return $this->findByPk($primaryKey);
	}
}