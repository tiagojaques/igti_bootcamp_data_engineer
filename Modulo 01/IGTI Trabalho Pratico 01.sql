USE ENEM2019

-- Q01 - Qual é a média da nota em matemática de todos os alunos mineiros?
SELECT AVG(NU_NOTA_MT) FROM prova_objetiva WHERE NU_NOTA_MT IS NOT null
-- R: 546.7962076512125

-- Q02 - Qual é a média da nota em Linguagens e Códigos de todos os alunos mineiros?
SELECT AVG(NU_NOTA_LC) FROM prova_objetiva WHERE NU_NOTA_LC IS NOT null
-- R: 531.2155500475544

-- Q03 - Qual é a média da nota em Ciências Humanas dos alunos do sexo FEMININO mineiros?
SELECT AVG(NU_NOTA_CH)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM participante WHERE TP_SEXO = 'F')
-- R: 515.1270983575349

-- Q04 - Qual é a média da nota em Ciências Humanas dos alunos do sexo MASCULINO?
SELECT AVG(NU_NOTA_CH)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM participante WHERE TP_SEXO = 'M')
-- R: 529.6982704731574
		  
-- Q05 - Qual é a média da nota em Matemática dos alunos do sexo FEMININO que moram na cidade de Montes Claros?
SELECT AVG(NU_NOTA_MT)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM participante WHERE TP_SEXO = 'F' AND NO_MUNICIPIO_RESIDENCIA = 'Montes Claros')
-- R: 525.4776724249901

-- Q06 - Qual é a média da nota em Matemática dos alunos do município de Sabará que possuem TV por assinatura na residência?
SELECT AVG(NU_NOTA_MT)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT a.NU_INSCRICAO
						   FROM participante AS a
						   INNER JOIN questionario_socioeconomico AS b
						   	   ON a.NU_INSCRICAO=b.NU_INSCRICAO
						   WHERE a.NO_MUNICIPIO_RESIDENCIA = 'Sabará' AND b.Q021 = 'B')
-- R: 543.2927556818183

-- Q07 - Qual é a média da nota em Ciências Humanas dos alunos mineiros que possuem dois fornos micro-ondas em casa?
SELECT AVG(NU_NOTA_CH)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM questionario_socioeconomico WHERE Q016 = 'C')
-- R: 557.2765986394558

-- Q08 - Qual é a nota média em Matemática dos alunos mineiros cuja mãe completou a pós-graduação?
SELECT AVG(NU_NOTA_MT)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM questionario_socioeconomico WHERE Q002 = 'G')
-- R: 620.007062070985

-- Q09 - Qual é a nota média em Matemática dos alunos de Belo Horizonte e de Conselheiro Lafaiete?
SELECT AVG(NU_NOTA_MT)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM participante WHERE NO_MUNICIPIO_RESIDENCIA IN ('Belo Horizonte', 'Conselheiro Lafaiete'))
-- R: 578.0392265100068
	
-- Q10 - Qual é a nota média em Ciências Humanas dos alunos mineiros que moram sozinhos?
SELECT AVG(NU_NOTA_CH)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM questionario_socioeconomico WHERE Q005 = 1)
-- R: 534.4573388609205

-- Q11 - Qual é a nota média em Ciências Humanas dos alunos mineiros cujo pai completou Pós graduação e possuem renda familiar entre R$ 8.982,01 e R$ 9.980,00.
SELECT AVG(NU_NOTA_CH)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM questionario_socioeconomico WHERE Q001 = 'G' AND Q006 = 'M')
-- R: 586.7231663685159
	
-- Q12 - Qual é a nota média em Matemática dos alunos do sexo Feminino que moram em Lavras e escolheram “Espanhol” como língua estrangeira?
SELECT AVG(NU_NOTA_MT)
FROM prova_objetiva
	WHERE TP_LINGUA = 1 AND
	      NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM participante WHERE TP_SEXO = 'F' AND NO_MUNICIPIO_RESIDENCIA = 'Lavras')
-- R: 510.80950782997775

-- Q13 - Qual é a nota média em Matemática dos alunos do sexo Masculino que moram em Ouro Preto?
SELECT AVG(NU_NOTA_MT)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM participante WHERE TP_SEXO = 'M' AND NO_MUNICIPIO_RESIDENCIA = 'Ouro Preto')
-- R: 555.0832520325198

-- Q14 - Qual é a nota média em Ciências Humanas dos alunos surdos?
SELECT AVG(NU_NOTA_CH)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT NU_INSCRICAO FROM atendimento_especializado WHERE IN_SURDEZ = 1)
-- R: 435.38796296296283
	
-- Q15 - Qual é a nota média em Matemática dos alunos do sexo FEMININO, que moram em Belo Horizonte, Sabará, Nova Lima e Betim e possuem dislexia?
SELECT AVG(NU_NOTA_MT)
FROM prova_objetiva
	WHERE NU_INSCRICAO IN (SELECT a.NU_INSCRICAO
						   FROM participante AS a 
						   INNER JOIN atendimento_especializado AS b 
							    ON a.NU_INSCRICAO = b.NU_INSCRICAO
						   WHERE a.TP_SEXO = 'F'
						     	AND a.NO_MUNICIPIO_RESIDENCIA IN ('Belo Horizonte', 'Sabará', 'Nova Lima', 'Betim')
						    	AND b.IN_DISLEXIA = 1)
-- R: 582.1935483870968
	