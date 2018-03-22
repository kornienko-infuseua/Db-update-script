<?php
require_once __DIR__ . '/../../vendor/autoload.php';
require_once __DIR__ . '/../../app/config.php';

ini_set('max_execution_time', 1000000);

ini_set('display_errors', 0);
ini_set('display_startup_errors', 1);
error_reporting(E_ERROR);

define('CREDENTIALS_PATH', __DIR__ . '/service-account-credentials.json');

$spreadsheetId = '1SxVEjLlU_cRxkiXLFH_QHfk1RqAIXLuas9Rm7lu9I3o';
//changes 1.1
class SpreadSheets
{
    public $service;
    public $sheets;
    public $rowValues;
    public $spreadsheetId;
    public $columnsNumbers;
    public $rowEmails;
    public $rowCompany;
    public $sleepSeconds;
    public $valid;
    public $rowNumber;

    public static $sheetIdPat = "@^https://docs.google.com/spreadsheets/d/([^/]*)/.*$@";
    public static $db = null;
    public static $processId;
    public static $mandatoryColumns = ['email', 'title', 'phone', 'prooflink', 'company', 'employees', 'employees_prooflink', 'revenue', 'revenue_prooflink'];

    public function __construct($spreadsheetId, $params = array())
    {
        $this->sleepSeconds = 1;
        $this->spreadsheetId = $spreadsheetId;
        $scopes = implode(' ', array(Google_Service_Sheets::SPREADSHEETS_READONLY)); // SPREADSHEETS SPREADSHEETS_READONLY

        $this->client = new Google_Client();
        $this->client->setAuthConfig(CREDENTIALS_PATH);
        $this->client->setApplicationName('inSegment lists');
        $this->client->setScopes($scopes);

        echo "<pre>\n";
        if (!isset(static::$db)) {
            echo "Connecting to DB\n";
            self::$db = new PDO('mysql:host=' . APP_DBHOST . ';dbname=' . APP_DBNAME, APP_DBUSER, APP_DBPASS);
            self::$db->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC);
            $res = self::$db->exec("INSERT INTO `processes` (`description`) VALUES ('lists google sheets')");

            if (!$res) {
                print_r(self::$db->errorInfo());
                exit();
            }

            self::$processId = self::$db->lastInsertId();
        }

        $this::checkStop();
        $this->service = new Google_Service_Sheets($this->client);
        $this->valid = $this->tryGetSheets($spreadsheetId, $params);
    }
// some changes made by Alexey
    public function tryGetSheets($spreadsheetId, $params)
    {
        try {
            $this->sheets = $this->service->spreadsheets->get($spreadsheetId, $params)->getSheets();
            $this->sleepSeconds = 1;
            $query = "UPDATE lists_google_sheets_links SET `status`=1 WHERE list_sheet_id = '$spreadsheetId'";
            self::$db->exec($query);
            return true;
        } catch (Google_Service_Exception $e) {
            $message = $e->getErrors()[0]['message'];
            $reason = $e->getErrors()[0]['reason'];
            if ($reason == 'forbidden') {
                echo "FORBIDDEN! $message for sheet: $spreadsheetId\n";
                $query = "UPDATE lists_google_sheets_links SET `status`=3, `comment` = '$message' WHERE list_sheet_id = '$spreadsheetId'";
                self::$db->exec($query);
                return false;
            }
            if ($reason == 'notFound') {
                echo "NOT FOUND! $message for sheet: $spreadsheetId\n";
                $query = "UPDATE lists_google_sheets_links SET `status`=5, `comment` = '$message' WHERE list_sheet_id = '$spreadsheetId'";
                self::$db->exec($query);
                return false;
            }

            if ($reason == 'backendError') {
                echo "BACKEND ERROR! $message for sheet: $spreadsheetId\n";
                $query = "UPDATE lists_google_sheets_links SET `status`=7, `comment` = '$message' WHERE list_sheet_id = '$spreadsheetId'";
                self::$db->exec($query);
                return false;
            }

            if ($reason == 'rateLimitExceeded') {
                echo "LIMIT EXCEEDED! $message for sheet: $spreadsheetId >>> ";

                if ($this->sleepSeconds > 80) {
                    $resStatus = self::$db->query("SELECT `status` FROM lists_google_sheets_links WHERE list_sheet_id = '$spreadsheetId'");
                    $oldStatus = $resStatus->fetch(\PDO::FETCH_COLUMN);

                    if ($oldStatus === 4) {
                        $query = "UPDATE lists_google_sheets_links SET `status`=6, `comment` = '$message' WHERE list_sheet_id = '$spreadsheetId'";
                    } else {
                        $query = "UPDATE lists_google_sheets_links SET `status`=4, `comment` = '$message' WHERE list_sheet_id = '$spreadsheetId'";
                    }
                    self::$db->exec($query);
                    echo " SKIPPING...########################...\n";
                    return false;
//                echo " EXIT...\n";
//                self::exitProcess();
                }

                sleep($this->sleepSeconds);
                $this->sleepSeconds *= 2;

                echo " RETRY\n";
                return $this->tryGetSheets($spreadsheetId, $params);
            } else {
                echo "UNKNOWN Error ", $reason, " ", $message;
                $query = "UPDATE lists_google_sheets_links SET `status`=8, `comment` = '$reason $message' WHERE list_sheet_id = '$spreadsheetId'";
                self::$db->exec($query);
                return false;
            }
        }
    }

    public function getRowData($sheetNumber = 0)
    {
        $sheet = $this->sheets[$sheetNumber];
        return $sheet->getData()[0]->getRowData();
    }

    public function getColumnCount()
    {
        return $this->sheets[0]->getProperties()->getGridProperties()->getColumnCount();
    }

    public static function exitProcess()
    {
        self::$db->exec("DELETE FROM `processes` WHERE `id`=" . self::$processId);
        exit;
    }

    public static function checkStop()
    {
        $query = "SELECT `stop` FROM `processes` WHERE `id`=" . self::$processId;
        $stop = self::$db->query($query)->fetch(\PDO::FETCH_COLUMN);
        if ($stop) {
            echo "\n-=STOP=-\n";
            self::exitProcess();
        }
    }

    public function getRowCount()
    {
        return $this->sheets[0]->getProperties()->getGridProperties()->getRowCount();
    }

    public function saveLinksToDB()
    {
        foreach ($this->sheets as $sheet) {
            $properties = $sheet->getProperties();
            $title = $properties->getTitle();
            $grigProperties = $properties->getGridProperties();
            $rowCount = $grigProperties->getRowCount();

            $range = $title . '!A1:Z1';
            $linksSpreadSheetsWData = new SpreadSheets($this->spreadsheetId, array('ranges' => $range, 'includeGridData' => true));
            $rowsData = $linksSpreadSheetsWData->getRowData(0);
            $rowData = $rowsData[0]; // row 0
            $cells = $rowData->getValues();
            if (!count($cells)) {
                continue;
            }

            $scriptColumn = '';
            foreach ($cells as $cellNum => $cell) {
                if (strtoupper($cell['formattedValue']) == 'SCRIPT') {
                    $scriptColumn = $this::getColumnLetter($cellNum);
                    break;
                }
            }

            $range = $title . '!E2:' . $scriptColumn . ($rowCount);
            $linksSpreadSheetsWData = new SpreadSheets($this->spreadsheetId, array('ranges' => $range, 'includeGridData' => true));
            $rowsData = $linksSpreadSheetsWData->getRowData(0);
            foreach ($rowsData as $rowData) {
                $cells = $rowData->getValues();
                if (!count($cells)) {
                    continue;
                }

                if ($cells[count($cells) - 1]['formattedValue'] == 'stop') {
                    break 2;
                }

                if ($cells[count($cells) - 1]['formattedValue'] != 'done') {
                    continue;
                }

                $value = $cells[0]['formattedValue'];
                if (!preg_match($this::$sheetIdPat, $value, $listSheetIdArr)) {
                    continue;
                }

                $listSheetId = addslashes($listSheetIdArr[1]);
                $link = addslashes($value);
                $title = addslashes($title);
                $query = "insert ignore into `lists_google_sheets_links` (list_sheet_id, `link`, links_sheet_title) values ('$listSheetId', '$link', '$title')";
                $res = self::$db->exec($query);
            }
        }
    }

    public function getLinksFromDB()
    {
        $query = "SELECT * FROM lists_google_sheets_links WHERE `status` = 0 OR `status` = 1 OR `status` = 4 OR `status` = 8";
        return self::$db->query($query)->fetchAll();
    }

    public function setDoneStatus()
    {
        $query = "UPDATE lists_google_sheets_links SET `status`=2 WHERE list_sheet_id = '{$this->spreadsheetId}'";
        return self::$db->exec($query);
    }

    public function makeColumnsNumbers($linkId, $columnsNumbers)
    {
        if ($columnsNumbers) {
            $this->columnsNumbers = $columnsNumbers;
            return $columnsNumbers;
        }

        $this->columnsNumbers = array();
        foreach ($this->rowValues as $columnKey => $value) {
            $this->columnsNumbers[strtolower($value['formattedValue'])] = $columnKey;
        }
        foreach ($this::$mandatoryColumns as $mandatoryColumn) {
            if (!isset($this->columnsNumbers[$mandatoryColumn])) {
                $this->saveMessage("\"$mandatoryColumn\" column not found", $linkId);
                //return false;
            }
        }
        return $this->columnsNumbers;
    }

    public function getRowEmail()
    {
        $columnName = 'email';
        if (!$this->isCellExists($columnName)) {
            return false;
        }

        $email = addslashes($this->rowValues[$this->columnsNumbers[$columnName]]['formattedValue']);
        if (!$email) {
            return false;
        }
        $query = "SELECT * FROM emails_status WHERE email = '$email'";
        $resEmails = self::$db->query($query);
        return $resEmails->fetch();
    }

    public function processEmail($linkId)
    {
        if (!$this->isCellExists('email')) {
            return false;
        }

        $emailCell = $this->rowValues[$this->columnsNumbers['email']];
        $emailColor = $this->getColor($emailCell);
        if ($emailColor === false) {
            return false;
        }

        $isStrikethrough = $emailCell->getEffectiveFormat()->getTextFormat()->getStrikethrough();

        if ($isStrikethrough) {
            $this->saveMessage("New email found", $linkId);
        }

        if ($emailColor == 'red') {
            $this->saveMessage("The contact is marked as bad data", $linkId);

            $query = "UPDATE `emails_status` e1\n"
                . "INNER JOIN `emails_status` e2 USING (`contact_id`)\n"
                . "SET e1.`valid` = 4, e1.`date_valid` = NOW()\n"
                . "WHERE e2.`id` = " . $this->rowEmails['id'];
            self::$db->exec($query);
            return false;
        }
        return true;
    }

    public function processCountry($linkId)
    {
        $colName = 'country';
        if (!$this->isCellExists($colName)) {
            return;
        }

        $cell = $this->rowValues[$this->columnsNumbers[$colName]];
        $cellColor = $this->getColor($cell);
        if ($cellColor == 'yellow' || $cellColor == 'green') {
            $newValue = addslashes($cell['formattedValue']);
            $resCountry = self::$db->query("SELECT id FROM countries WHERE name = '$newValue'");
            if (!$resCountry) {
                $resCountry = self::$db->query("SELECT country_id FROM countries_aliases WHERE alias = '$newValue'");
            }
            if ($resCountry) {
                $newCountryId = $resCountry->fetch(\PDO::FETCH_COLUMN);
                $updated = self::$db->exec("UPDATE emails_status set country_id=$newCountryId WHERE email = '{$this->rowEmails['email']}'");
                if ($updated > 0) {
                    $query = "UPDATE `contacts` SET `address` = '', `city` = '', `state` = '', `zip` = '' WHERE `id` = '{$this->rowEmails['contact_id']}'";
                    self::$db->exec($query);
                }
            } else {
                $this->saveMessage("Country not found: {$newValue}", $linkId);
            }
//            $query = "UPDATE `contacts` SET `$colName` = '$newValue' WHERE `id` = '{$this->rowEmails['contact_id']}'";
//            self::$db->exec($query);
        }
    }

    public function processTitle($linkId)
    {
        $columnName = 'title';
        if (!$this->isCellExists($columnName)) {
            return;
        }

        $titleCell = $this->rowValues[$this->columnsNumbers[$columnName]];
        $titleColor = $this->getColor($titleCell);
        if ($titleColor == 'yellow' || $titleColor == 'green') {
            if (!$this->isCellExists('prooflink')) {
                $this->saveMessage("Title color is $titleColor, but Prooflink is empty", $linkId);
                return;
            }
            $prooflinkCell = $this->rowValues[$this->columnsNumbers['prooflink']];
            $newProoflink = $prooflinkCell['formattedValue'];
            if (empty($newProoflink)) {
                $this->saveMessage("Title color is $titleColor, but Prooflink is empty", $linkId);
                return;
            }
            $prooflinkColor = $this->getColor($prooflinkCell);
            if ($prooflinkColor != 'yellow' && $prooflinkColor != 'green') {
                $this->saveMessage("Title color is $titleColor, but Prooflink color is $prooflinkColor", $linkId);
            }
            $newTitle = addslashes($titleCell['formattedValue']);
            $query = "SELECT id FROM titles WHERE title = '$newTitle'";
            $resTitle = self::$db->query($query);
            $newTitleId = $resTitle->fetch(\PDO::FETCH_COLUMN);
            if (!$newTitleId) {
                $query = "insert into `titles` (`title`, `count`) values ('$newTitle', 1)";
                $res = self::$db->exec($query);
                $newTitleId = self::$db->lastInsertId();
            }
            /*
                        if ($newTitleId == $this->rowEmails['title_id']) {
                            return;
                        }
            */
            $query = "UPDATE emails_status set title_id = $newTitleId WHERE id = " . $this->rowEmails['id'];
            $res = self::$db->exec($query);
        }
    }

    public function processPhone()
    {
        $colName = 'phone';
        if (!$this->isCellExists($colName)) {
            return;
        }

        $phoneCell = $this->rowValues[$this->columnsNumbers[$colName]];
        $phoneColor = $this->getColor($phoneCell);
        if ($phoneColor == 'yellow' || $phoneColor == 'green') {
            $newValue = addslashes($phoneCell['formattedValue']);
            if (!empty($newValue)) {
                $query = "INSERT INTO `phones` (`contact_id`, `original`) VALUES ('{$this->rowEmails['contact_id']}', '{$newValue}') ON DUPLICATE KEY UPDATE `original` =  '{$newValue}';";
                self::$db->exec($query);
            }

            $commentColName = false;
            if ($this->isCellExists('pv comment')) {
                $commentColName = 'pv comment';
            }
            if ($this->isCellExists('PV comment')) {
                $commentColName = 'PV comment';
            }
            if (!$commentColName) {
                return;
            }

            $callDateColName = false;
            if ($this->isCellExists('call date')) {
                $callDateColName = 'call date';
            }
            if ($this->isCellExists('call_date')) {
                $callDateColName = 'call_date';
            }
            if (!$callDateColName) {
                return;
            }

            $callDateCell = $this->rowValues[$this->columnsNumbers[$callDateColName]];
            $callDate = $callDateCell['formattedValue'];
            if (empty($callDate)) {
                return;
            }

            $pvCommentCell = $this->rowValues[$this->columnsNumbers[$commentColName]];
            $pvCommentValue = $pvCommentCell['formattedValue'];
            $pvCommentsArr = array(
                'LC (CD)',
                'LC (direct/transferred)',
                'LC (operator/not transferred)',
                'LC (operator/transfer failed)',
            );
            if (in_array($pvCommentValue, $pvCommentsArr)) {
                $callDate = date('Y-m-d', strtotime($callDate));
                $query = "UPDATE emails_status set phone_verified='{$callDate}' WHERE id = " . $this->rowEmails['id'];
                self::$db->exec($query);
            }
        }
    }

    public function processProoflink()
    {
        if (!$this->isCellExists('prooflink')) {
            return;
        }

        $prooflinkCell = $this->rowValues[$this->columnsNumbers['prooflink']];
        $prooflinkColor = $this->getColor($prooflinkCell);
        if ($prooflinkColor == 'yellow' || $prooflinkColor == 'green') {
            $emailId = $this->rowEmails['id'];
            $newLink = addslashes($prooflinkCell['formattedValue']);
            $query = "INSERT INTO prooflinks (`id`, `link`, `date`) values ($emailId, '$newLink', now()) ON DUPLICATE KEY UPDATE `link`='$newLink', `date`=now()";
            $res = self::$db->exec($query);

            /*
            $query = "SELECT * FROM prooflinks WHERE id = $emailId";
            $resProoflinks = self::$db->query($query)->fetch();
            if ($resProoflinks['link'] != $newLink) {
                echo 'prooflink old: ', $resProoflinks['link'], " \t new: ", $newLink, " \t color: ", $prooflinkColor, " \t ";
            }
            */
        }
    }

    public function processColumnByName($colName)
    {
        if (!$this->isCellExists($colName)) {
            return;
        }

        $cell = $this->rowValues[$this->columnsNumbers[$colName]];
        $cellColor = $this->getColor($cell);
        if ($cellColor == 'yellow' || $cellColor == 'green') {
            $newValue = addslashes($cell['formattedValue']);
            $query = "UPDATE `contacts` SET `$colName` = '$newValue' WHERE `id` = '{$this->rowEmails['contact_id']}'";
            self::$db->exec($query);
        }
    }

    public function saveMessage($message, $linkId)
    {
        $query = "INSERT IGNORE INTO lists_google_sheets_qa (company_id, link_id, message, row_number) values ('{$this->rowEmails['company_id']}', '$linkId', '$message', '{$this->rowNumber}')";
        $res = self::$db->exec($query);
        if ($res === null) {
            echo $query, "\t";
        }

        echo $message, ', res=', $res, "\n";
    }

    public function processCompany($linkId)
    {
        if (!$this->isCellExists('company')) {
            return;
        }

        $companyCell = $this->rowValues[$this->columnsNumbers['company']];
        $companyColor = $this->getColor($companyCell);

        $query = "SELECT * FROM companies WHERE id=" . $this->rowEmails['company_id'];
        $this->rowCompany = self::$db->query($query)->fetch();

        $fileCompanyName = trim($companyCell['formattedValue']);
        $dbCompanyName = trim($this->rowCompany['name']);
        $find = array('.', ',');
        $dbLowCompanyName = strtolower(str_replace($find, '', $dbCompanyName));
        $fileLowCompanyName = strtolower(str_replace($find, '', $fileCompanyName));
        $sqlFileCompanyName = addslashes($fileCompanyName);
        $sqlDbCompanyName = addslashes($this->rowCompany['name']);

        if ($dbLowCompanyName != $fileLowCompanyName) {
            /*            $query = "SELECT id FROM companies_aliases WHERE company_id=" . $this->rowEmails['company_id']
                            . " and alias='$sqlFileCompanyName'";*/
            $query = "SELECT 1\n" .
                "FROM `companies_aliases` t1\n" .
                "INNER JOIN `companies_aliases` t2\n" .
                "ON t1.company_id = t2.company_id\n" .
                "AND t1.`alias` = '{$sqlDbCompanyName}'\n" .
                "AND t2.`alias` = '{$sqlFileCompanyName}'\n" .
                "UNION\n" .
                "SELECT 1\n" .
                "FROM `companies` c\n" .
                "INNER JOIN `companies_aliases` a\n" .
                "ON c.id = a.company_id\n" .
                "AND c.`name` = '{$sqlDbCompanyName}'\n" .
                "AND a.`alias` = '{$sqlFileCompanyName}'\n" .
                "OR c.`name` = '{$sqlFileCompanyName}'\n" .
                "AND a.`alias` = '{$sqlDbCompanyName}'\n" .
                "LIMIT 1";
            $foundCompanyAlias = self::$db->query($query)->fetch(\PDO::FETCH_COLUMN);

            if (!$foundCompanyAlias) {
                $this->saveMessage("Company is changed. In DB: \"{$dbCompanyName}\", "
                    . "in file: \"{$fileCompanyName}\". Color is $companyColor", $linkId);

                if ($companyColor == 'yellow' || $companyColor == 'green') {
                    $query = "SELECT id FROM companies WHERE `name`='$sqlFileCompanyName'";
                    $newCompanyId = self::$db->query($query)->fetch(\PDO::FETCH_COLUMN);
                    if (!$newCompanyId) {
                        $query = "SELECT company_id FROM companies_aliases WHERE alias='$sqlFileCompanyName'";
                        $newCompanyId = self::$db->query($query)->fetch(\PDO::FETCH_COLUMN);
                    }
                    if (!$newCompanyId) {
                        self::$db->exec("INSERT INTO `companies` (`name`, `count`) VALUES ('$sqlFileCompanyName', 1)");
                        $newCompanyId = self::$db->lastInsertId();
                    }
                    self::$db->exec("UPDATE emails_status set company_id='$newCompanyId' WHERE id = '{$this->rowEmails['id']}'");
                    $this->rowEmails['company_id'] = $newCompanyId;
                }
            }
        };

        $this->processEmployeesProoflink($linkId);
        $this->processRevenueProoflink($linkId);
    }

    public function processEmployees($linkId)
    {
        if (!$this->isCellExists('employees')) {
            return;
        }

        $employeesCell = $this->rowValues[$this->columnsNumbers['employees']];
        $employeesColor = $this->getColor($employeesCell);
        if ($employeesColor == 'yellow' || $employeesColor == 'green') {
            /*            if (!$this->isCellExists('employees_prooflink')) {
                            $this->saveMessage("Employees color is $employeesColor, but Employees Prooflink is empty", $linkId);
                            return;
                        }*/
            $employeesProoflinkCell = $this->rowValues[$this->columnsNumbers['employees_prooflink']];
            $newEmployeesProoflink = $employeesProoflinkCell['formattedValue'];
            if (empty($newEmployeesProoflink)) {
                $this->saveMessage("Employees color is $employeesColor, but Employees Prooflink is empty", $linkId);
                return;
            }
            list($employeesMin, $employeesMax) = $this::getMinMax($employeesCell['formattedValue']);
            if ($this->rowCompany['verified'] > 0 && ($this->rowCompany['employees_min'] != $employeesMin || $this->rowCompany['employees_max'] != $employeesMax)) {
                $this->saveMessage("Verified is L or V or VM and Employees is changed", $linkId);
                return;
            }

            $query = "UPDATE companies SET employees_min=$employeesMin, employees_max=$employeesMax WHERE id = " . $this->rowEmails['company_id'];
            self::$db->exec($query);
        }
    }

    public function processEmployeesProoflink($linkId)
    {
        if (!$this->isCellExists('employees_prooflink')) {
            return;
        }

        $employeesProoflinkCell = $this->rowValues[$this->columnsNumbers['employees_prooflink']];
        $employeesProoflinkColor = $this->getColor($employeesProoflinkCell);
        if ($employeesProoflinkColor == 'yellow' || $employeesProoflinkColor == 'green') {
            $tmpArr = explode("?", $employeesProoflinkCell['formattedValue']);
            $newEmployeesProoflink = $tmpArr[0];
            $tmpArr = explode("?", $this->rowCompany['employees_prooflink']);
            $oldEmployeesProoflink = $tmpArr[0];
            if ($oldEmployeesProoflink != $newEmployeesProoflink && $oldEmployeesProoflink) {
                $this->saveMessage("Employees Prooflink is changed", $linkId);
                return;
            }
            if ($newEmployeesProoflink && !$oldEmployeesProoflink) {
                $newEmployeesProoflink = addslashes($employeesProoflinkCell['formattedValue']);
                $query = "UPDATE companies SET employees_prooflink='$newEmployeesProoflink' WHERE id = " . $this->rowEmails['company_id'];
                self::$db->exec($query);
                $this->saveMessage("Employees Prooflink is added", $linkId);
            }
            $this->processEmployees($linkId);
        }
    }

    public function processRevenue($linkId)
    {
        if (!$this->isCellExists('revenue')) {
            return false;
        }

        $revenueCell = $this->rowValues[$this->columnsNumbers['revenue']];
        $revenueColor = $this->getColor($revenueCell);
        if ($revenueColor == 'yellow' || $revenueColor == 'green') {
            /*            if (!$this->isCellExists('revenue_prooflink')) {
                            $this->saveMessage("Revenue Prooflink is empty", $linkId);
                            return;
                        }*/
            $revenueProoflinkCell = $this->rowValues[$this->columnsNumbers['revenue_prooflink']];
            $newRevenueProoflink = $revenueProoflinkCell['formattedValue'];
            if (empty($newRevenueProoflink)) {
                $this->saveMessage("Revenue Prooflink is empty", $linkId);
                return;
            }
            list($revenueMin, $revenueMax) = $this::getMinMax($revenueCell['formattedValue']);
            if ($this->rowCompany['verified'] > 1 && ($this->rowCompany['revenue_min'] != $revenueMin || $this->rowCompany['revenue_max'] != $revenueMax)) {
                $this->saveMessage("Verified is V or VM and Revenue is changed", $linkId);
                return;
            }

            list($revenueMin, $revenueMax) = $this::getMinMax($revenueCell['formattedValue']);
            $query = "UPDATE companies SET revenue_min=$revenueMin, revenue_max=$revenueMax WHERE id = " . $this->rowEmails['company_id'];
            self::$db->exec($query);
        }
    }

    public function processRevenueProoflink($linkId)
    {
        $columnName = 'revenue_prooflink';
        if (!$this->isCellExists($columnName)) {
            return false;
        }

        $revenueProoflinkCell = $this->rowValues[$this->columnsNumbers[$columnName]];
        $revenueProoflinkColor = $this->getColor($revenueProoflinkCell);
        if ($revenueProoflinkColor == 'yellow' || $revenueProoflinkColor == 'green') {
            $tmpArr = explode("?", $revenueProoflinkCell['formattedValue']);
            $newRevenueProoflink = $tmpArr[0];
            $tmpArr = explode("?", $this->rowCompany['revenue_prooflink']);
            $oldRevenueProoflink = $tmpArr[0];
            if ($oldRevenueProoflink != $newRevenueProoflink && $oldRevenueProoflink) {
                $this->saveMessage("Revenue Prooflink is changed", $linkId);
                return;
            }

            if ($newRevenueProoflink && !$oldRevenueProoflink) {
                $newRevenueProoflink = addslashes($revenueProoflinkCell['formattedValue']);
                $query = "UPDATE companies SET revenue_prooflink='$newRevenueProoflink' WHERE id = " . $this->rowEmails['company_id'];
                self::$db->exec($query);
                $this->saveMessage("Revenue Prooflink is added", $linkId);
            }
            $this->processRevenue($linkId);
        }
    }

    public function getColor($cell)
    {
        $format = $cell->getEffectiveFormat();
        if (!$format) {
            return false;
        }
        $bgColor = $format->getBackgroundColor();
        $rgb = dechex(
            ($bgColor->getRed() * 255 * 256 + $bgColor->getGreen() * 255)
            * 256 + $bgColor->getBlue() * 255
        );
        if ($rgb === 'ffffff') {
            return 'white';
        }
        if ($rgb === 'e06666') {
            return 'red';
        }
        if ($rgb === '93c47d') {
            return 'green';
        }
        if ($rgb === 'ffff00') {
            return 'yellow';
        }

        return 'unknown';
    }

    public function isCellExists($columnName)
    {
        if (!isset($this->columnsNumbers[$columnName])) {
            return false;
        }
        if (count($this->rowValues) <= $this->columnsNumbers[$columnName]) {
            return false;
        }

        return true;
    }

    public static function getMinMax($str)
    {
        $arr = explode('-', $str);
        $min = $arr[0];
        if (count($arr) == 1) {  //  '10000+' || '10000'
            if ($min == '' . intval($min)) {  //  '10000'
                $max = $min;
            } else {   //  '10000+'
                $max = 0;
            }
        } else { //  '1-1000'
            $max = $arr[1];
        }

        $min = intval($min);
        return array($min, $max);
    }

    public static function getColumnLetter($index)
    {
        $alphabet = str_split('ABCDEFGHIJKLMNOPQRSTUVWXYZ');
        $length = count($alphabet);
        $result = '';
        while ($index >= 0) {
            $result = $alphabet[$index % $length] . $result;
            $index = intval($index / $length) - 1;
        };
        return $result;
    }
}// end class

register_shutdown_function("fatal_handler");

/*///////////////////////////////////////////////
$testId = '1bS-DvJMTw0_x4AOGntQg9hClJFnvnaJMI-I2Izd9m0w';

$testEditSpreadsheet = new SpreadSheets($testId,
    array('ranges' => 'A1:B2', 'includeGridData' => true));
$rowsData = $testEditSpreadsheet->getRowData(0);
$rowData = $rowsData[0];
$cellData = $rowData->getValues();
//$cellData[0]->setFormattedValue(8888);
//$val = $cellData[0]->getFormattedValue();
print_r($rowsData);











//$format = '0.0%';
//$sheetId = 0;
//
//$numberFormat = new Google_Service_Sheets_NumberFormat();
//$numberFormat->setType('NUMBER');
//$numberFormat->setPattern($format);
//$cellFormat = new Google_Service_Sheets_CellFormat();
//$cellFormat->setNumberFormat($numberFormat);
//$cellData = new Google_Service_Sheets_CellData();
//$cellData->setUserEnteredFormat($cellFormat);
//$rowData = new Google_Service_Sheets_RowData();
//$rowData->setValues([$cellData]);
//$rows[] = $rowData;
//
//$gridRange = new Google_Service_Sheets_GridRange();
//$gridRange->setSheetId($sheetId);
//$gridRange->setStartRowIndex(0);
//$gridRange->setEndRowIndex(1);
//$gridRange->setStartColumnIndex(0);
//$gridRange->setEndColumnIndex(1);
//
//$fields = 'userEnteredFormat.numberFormat';
//
//$updateCellsRequest = new Google_Service_Sheets_UpdateCellsRequest();
//$updateCellsRequest->setFields($fields);
//$updateCellsRequest->setRows($rows);
//$updateCellsRequest->setRange($gridRange);
//
//$request = new Google_Service_Sheets_Request();
//$request->setUpdateCells($updateCellsRequest);
//$batchUpdate = new Google_Service_Sheets_BatchUpdateSpreadsheetRequest();
//$batchUpdate->setRequests([$request]);
//
//$testEditSpreadsheet->service->spreadsheets->batchUpdate($testId, $batchUpdate);















$updateCellsRequest = new Google_Service_Sheets_UpdateCellsRequest(array(
    'fields' => '*',
    'start'  => new Google_Service_Sheets_GridCoordinate(array(
        'sheetId' => 0,
        'rowIndex' => 10,
        'columnIndex' => 10
    ))
));

$rowData = array();
$rowData = $rowsData;
$sheetRowData = new \Google_Service_Sheets_RowData();
//$cellData = array();

$value = 12313412;
$sheetCellData = new \Google_Service_Sheets_CellData(array(
    'userEnteredValue' => new \Google_Service_Sheets_ExtendedValue(array(
        is_numeric($value) ? 'numberValue' : 'stringValue' => $value
    ))
));
//$cellData[] = $sheetCellData;

$sheetRowData->setValues($cellData);
//$rowData[] = $sheetRowData;

$updateCellsRequest->setRows($rowData);

// Create a batch update request
$updateRequest = new \Google_Service_Sheets_BatchUpdateSpreadsheetRequest(array(
    'requests' => array(
        new \Google_Service_Sheets_Request(array('updateCells' => $updateCellsRequest))
    )
));

// send the request
$testEditSpreadsheet->service->spreadsheets->batchUpdate($testId, $updateRequest);





//$values = array(
//    array(
//        '10', '120'
//    ),
//    array(
//        '20', '130'
//    ),
//    // Additional rows ...
//);
//
//$range = 'A1:B2';
//$body = new Google_Service_Sheets_ValueRange(array(
//    'values' => $values
//));
//$params = array(
//    'valueInputOption' => 'RAW'
//);
//$result = $testEditSpreadsheet->service->spreadsheets_values->update($testId, $range,
//    $body, $params);








exit;
/////////////////////////////////////*/

$linksSpreadSheets = new SpreadSheets($spreadsheetId);
$linksSpreadSheets->saveLinksToDB();
$linksRows = $linksSpreadSheets->getLinksFromDB();

foreach ($linksRows as $linksRow) {
    $listSheetId = $linksRow['list_sheet_id'];
    echo $listSheetId, "\t", $linksRow['links_sheet_title'], "\n";
    if (isset($_GET['id'])) {
        if ($listSheetId != $_GET['id']) {
            continue;
        }
    }

    $listSpreadSheets_ = new SpreadSheets($listSheetId);
    if (!$listSpreadSheets_->valid) {
        continue;
    }
    $rowCount = $listSpreadSheets_->getRowCount();
    $columnCount = $listSpreadSheets_->getColumnCount();

    $columnsNumbers = false;
    $limit = 50;
    for ($rowIndex = 1; $rowIndex <= $rowCount; $rowIndex += $limit) {
        $range = 'A' . $rowIndex . ':' . SpreadSheets::getColumnLetter($columnCount - 1) . ($rowIndex + $limit - 1);
        echo $range;
        $listSpreadSheets = new SpreadSheets($listSheetId, array('ranges' => $range, 'includeGridData' => true)); // Here who eating memory
        if (!$listSpreadSheets->valid) {
            echo " Not valid now. Continue next\n";
            continue;
        }
        echo " loaded \n";
        $rowsData = $listSpreadSheets->getRowData(0); // only first sheet. Maximum execution time of 120 seconds exceeded
        foreach ($rowsData as $rowKey => $rowData) {
            $listSpreadSheets->rowValues = $rowData->getValues();
            if (!count($listSpreadSheets->rowValues)) {
                continue;
            }

            $listSpreadSheets->rowNumber = $rowIndex + $rowKey;
            if ($listSpreadSheets->rowNumber % 50 == 1) {
                $columnsNumbers = $listSpreadSheets->makeColumnsNumbers($linksRow['id'], $columnsNumbers);
                if ($listSpreadSheets->rowNumber == 1) {
                    continue;
                }
            }

            $listSpreadSheets->rowEmails = $listSpreadSheets->getRowEmail(); // Фигово
            if (!$listSpreadSheets->rowEmails) {
                continue;
            }

            if (!$listSpreadSheets->processEmail($linksRow['id'])) {
                continue;
            }

            $listSpreadSheets->processColumnByName('first_name');
            $listSpreadSheets->processColumnByName('last_name');

            $listSpreadSheets->processCountry($linksRow['id']);
            $listSpreadSheets->processColumnByName('address');
            $listSpreadSheets->processColumnByName('city');
            $listSpreadSheets->processColumnByName('state');
            $listSpreadSheets->processColumnByName('zip');

            $listSpreadSheets->processTitle($linksRow['id']);
            $listSpreadSheets->processPhone();
            $listSpreadSheets->processProoflink();
            $listSpreadSheets->processCompany($linksRow['id']);

            echo ".\n";
        }
        echo "\n";
        flush();
    }
    $listSpreadSheets_->setDoneStatus();
}

echo "\nDone Sucessfully! Please restart to recheck\n";

$linksSpreadSheets::exitProcess();

function fatal_handler()
{
    $error = error_get_last();
    if ($error !== null) {
        print_r($error);
    }
    SpreadSheets::exitProcess();
}
