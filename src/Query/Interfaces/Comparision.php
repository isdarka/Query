<?php
/**
 *
 * @author isdarka
 * @created Nov 22, 2013 5:26:25 PM
 */

namespace Query\Interfaces;

interface Comparision
{

	const EQUAL = '=';
	const JUST_LIKE = 'LIKE'; #LIKE '{value}'
	const LIKE = ' LIKE '; #LIKE '%{value}%'
	const LEFT_LIKE = ' LIKE'; #LIKE '%{value}'
	const RIGHT_LIKE = 'LIKE '; #LIKE '{value}%'
	const NOT_LIKE = ' NOT_LIKE '; #NOT LIKE '%{value}%'
	const NOT_JUST_LIKE = 'NOT_LIKE'; #NOT LIKE '{value}'
	const IN = 'IN';
	const NOT_IN = 'NOT IN';
	const NOT_EQUAL = '!=';
	const GREATER_THAN = '>';
	const LESS_THAN = '<';
	const GREATER_OR_EQUAL = '>=';
	const LESS_OR_EQUAL = '<=';
	const BETWEEN = 'BETWEEN';
	const IS_NULL = 'IS NULL';
	const IS_NOT_NULL = 'IS NOT NULL';
	const RANGE = 'RANGE';
	const AUTO = 'AUTO';
	
	const MD5 = "MD5('%s')";
	/**********/
	const ASC = 'ASC';
	const DESC = 'DESC';
}