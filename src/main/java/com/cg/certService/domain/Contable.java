package com.cg.certService.domain;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class Contable {

	
	@JsonFormat(pattern="dd/MM/yyyy")
	private Date periodo;
	
	@JsonFormat(pattern="dd/MM/yyyy")
	private Date fecha_cierre;
	
	private long tstamp;

	public Date getPeriodo() {
		return periodo;
	}

	public void setPeriodo(Date periodo) {
		this.periodo = periodo;
	}

	public Date getFecha_cierre() {
		return fecha_cierre;
	}

	public void setFecha_cierre(Date fecha_cierre) {
		this.fecha_cierre = fecha_cierre;
	}

	public long getTstamp() {
		return tstamp;
	}

	public void setTstamp(long tstamp) {
		this.tstamp = tstamp;
	}
	
	
}
