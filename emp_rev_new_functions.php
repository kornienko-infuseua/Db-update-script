    public function processEmployees($linkId)
    {
        if (!$this->isCellExists('employees')) {
            return;
        }

        $employeesCell = $this->rowValues[$this->columnsNumbers['employees']];
        $employeesColor = $this->getColor($employeesCell);
        if ($employeesColor == 'yellow' ) {
            list($employeesMin, $employeesMax) = $this::getMinMax($employeesCell['formattedValue']);
            $query = "UPDATE companies SET employees_min=$employeesMin, employees_max=$employeesMax WHERE id = " . $this->rowEmails['company_id'];
            self::$db->exec($query);
        }
    }

    public function processEmployeesProoflink($linkId)
    {
        if (!$this->isCellExists('employees_prooflink') ) {
            return;
        }

        $employeesProoflinkCell = $this->rowValues[$this->columnsNumbers['employees_prooflink']];
        $employeesProoflinkColor = $this->getColor($employeesProoflinkCell);
		$tmpArr = explode("?", $employeesProoflinkCell['formattedValue']);
        $newEmployeesProoflink = $tmpArr[0];
         if ( !empty($newEmployeesProoflink) && ($employeesProoflinkColor == 'yellow' || $employeesProoflinkColor == 'green') ){
          
            $tmpArr = explode("?", $this->rowCompany['employees_prooflink']);
            $oldEmployeesProoflink = $tmpArr[0];
			 if ($oldEmployeesProoflink != $newEmployeesProoflink ) {
				if ($employeesProoflinkColor == 'green') return;
				if ($this->rowCompany['verified'] == 3) 
					$this->saveMessage("New Employees PL for VM company", $linkId);
				else  {
					$newEmployeesProoflink = addslashes($employeesProoflinkCell['formattedValue']);
					$query = "UPDATE companies SET employees_prooflink='$newEmployeesProoflink' WHERE id = " . $this->rowEmails['company_id'];
					self::$db->exec($query);	
					$this->processEmployees($linkId);
					
				}
            }
			else $this->processEmployees($linkId);
        }
    }

    public function processRevenue($linkId)
    {
        if (!$this->isCellExists('revenue')) {
            return false;
        }

        $revenueCell = $this->rowValues[$this->columnsNumbers['revenue']];
        $revenueColor = $this->getColor($revenueCell);
        if ($revenueColor == 'yellow' ) {
           
            list($revenueMin, $revenueMax) = $this::getMinMax($revenueCell['formattedValue']);
            $query = "UPDATE companies SET revenue_min=$revenueMin, revenue_max=$revenueMax WHERE id = " . $this->rowEmails['company_id'];
            self::$db->exec($query);
        }
    }

    public function processRevenueProoflink($linkId)
    {
        if (!$this->isCellExists('revenue_prooflink') ) {
            return;
        }

        $revenueProoflinkCell = $this->rowValues[$this->columnsNumbers['revenue_prooflink']];
        $revenueProoflinkColor = $this->getColor($revenueProoflinkCell);
		$tmpArr = explode("?", $revenueProoflinkCell['formattedValue']);
        $newRevenueProoflink = $tmpArr[0];
        if ( !empty($newRevenueProoflink) && ($revenueProoflinkColor == 'yellow' || $revenueProoflinkColor == 'green') ){
            
            $tmpArr = explode("?", $this->rowCompany['revenue_prooflink']);
            $oldRevenueProoflink = $tmpArr[0];
			 if ($oldRevenueProoflink != $newRevenueProoflink ) {
				if ($revenueProoflinkColor == 'green') return;
				if ($this->rowCompany['verified'] == 3) 
					$this->saveMessage("New Revenue PL for VM company", $linkId);
				else  {
					$newRevenueProoflink = addslashes($revenueProoflinkCell['formattedValue']);
					$query = "UPDATE companies SET revenue_prooflink='$newRevenueProoflink' WHERE id = " . $this->rowEmails['company_id'];
					self::$db->exec($query);			
					$this->processRevenue($linkId);
				}
            }
			else $this->processRevenue($linkId);
        }
    }
